package com.iblync.kafka.connect.bigquery.sink.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.iblync.kafka.connect.bigquery.sink.ConcurrencyHint;
import com.iblync.kafka.connect.bigquery.sink.SinkAction;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.sink.handler.BigQueryHandler;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerContext;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerParams;
import com.iblync.kafka.connect.bigquery.util.TopicMap;

import reactor.core.publisher.Mono;

public class UpdateOneRow implements BigQueryHandler, Closeable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(UpdateOneRow.class);
	private BigQuery bigQuery;
	private BigQueryConfig config;
	private Map<String, String> topicToTableMap;
	private Map<String, String> topicToDatasetMap;
	private Map<String, String> topicToPkMap;
	
	@Override
	public void init(SinkHandlerContext context, BigQueryConfig config) {
		this.config = config;
		Map<String, String> props = context.configProperties();
		
		// Parse topic-to-table, topic-to-dataset, and topic-to-pk mappings using TopicMap utility
		this.topicToTableMap = TopicMap.parseTopicToValues(config.topicToTable(), null);
		this.topicToDatasetMap = TopicMap.parseTopicToValues(config.topicToDataset(), config.dataset());
		this.topicToPkMap = TopicMap.parseTopicToValues(config.topicToPk(), "id");
		
		// Initialize BigQuery client
		String keyFilePath = props.get("bigquery.keyfile");
		try {
			if (keyFilePath != null && !keyFilePath.isEmpty()) {
				this.bigQuery = BigQueryOptions.newBuilder()
						.setProjectId(config.project())
						.setCredentials(com.google.auth.oauth2.ServiceAccountCredentials.fromStream(
								getClass().getResourceAsStream(keyFilePath)))
						.build()
						.getService();
			} else {
				// Use default credentials
				this.bigQuery = BigQueryOptions.newBuilder()
						.setProjectId(config.project())
						.build()
						.getService();
			}
		} catch (IOException e) {
			LOGGER.error("Failed to initialize BigQuery client", e);
			throw new RuntimeException("Failed to initialize BigQuery client", e);
		}
		
		LOGGER.info("UpdateOneRow initialized for project: {}, topicToTable: {}, topicToDataset: {}, topicToPk: {}", 
				config.project(), topicToTableMap, topicToDatasetMap, topicToPkMap);
	}

	@Override
	public void close() throws IOException {
		if (bigQuery != null) {
			// BigQuery client doesn't need explicit closing
			LOGGER.info("UpdateOneRow closed");
		}
	}

	@Override
	public SinkAction handle(SinkHandlerParams params) {
		SinkRecord record = params.sinkRecord();
		String topic = record.topic();
		
		// Get table, dataset, and primary key for this topic
		String tableName = topicToTableMap.get(topic);
		String datasetName = topicToDatasetMap.getOrDefault(topic, config.dataset());
		String primaryKey = topicToPkMap.getOrDefault(topic, "id");
		
		if (tableName == null) {
			LOGGER.warn("No table mapping found for topic: {}", topic);
			return SinkAction.ignore();
		}
		
		return new SinkAction(
			Mono.fromCallable(() -> {
				try {
					// Convert record value to Map
					Map<String, Object> rowData = convertRecordToMap(record);
					
					// Build UPDATE SQL query
					String updateQuery = buildUpdateQuery(config.project(), datasetName, tableName, rowData, primaryKey);
					
					// Create and execute query job
					QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(updateQuery)
							.setUseLegacySql(false)
							.build();
					
					JobId jobId = JobId.of(java.util.UUID.randomUUID().toString());
					Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
					
					// Wait for the query to complete
					queryJob = queryJob.waitFor();
					
					if (queryJob == null) {
						throw new RuntimeException("Job no longer exists");
					} else if (queryJob.getStatus().getError() != null) {
						throw new RuntimeException("BigQuery update failed: " + queryJob.getStatus().getError());
					}
					
					LOGGER.debug("Successfully updated record in BigQuery table: {}.{}.{}", 
							config.project(), datasetName, tableName);
					return null;
					
				} catch (Exception e) {
					LOGGER.error("Error updating record in BigQuery for topic: {}", topic, e);
					throw new RuntimeException("Error updating record in BigQuery", e);
				}
			}),
			ConcurrencyHint.of(record.topic() + "-" + record.kafkaPartition())
		);
	}
	
	private String buildUpdateQuery(String projectId, String datasetName, String tableName, 
			Map<String, Object> rowData, String primaryKey) {
		StringBuilder query = new StringBuilder();
		query.append("UPDATE `").append(projectId).append(".").append(datasetName).append(".").append(tableName).append("` SET ");
		
		boolean first = true;
		Object primaryKeyValue = null;
		
		for (Map.Entry<String, Object> entry : rowData.entrySet()) {
			String column = entry.getKey();
			Object value = entry.getValue();
			
			if (column.equals(primaryKey)) {
				primaryKeyValue = value;
				continue; // Don't update the primary key
			}
			
			if (column.startsWith("_kafka_")) {
				continue; // Skip metadata fields in SET clause
			}
			
			if (!first) {
				query.append(", ");
			}
			
			query.append("`").append(column).append("` = ");
			if (value == null) {
				query.append("NULL");
			} else if (value instanceof String) {
				query.append("'").append(value.toString().replace("'", "\\'")).append("'");
			} else if (value instanceof Number) {
				query.append(value);
			} else {
				query.append("'").append(value.toString().replace("'", "\\'")).append("'");
			}
			
			first = false;
		}
		
		query.append(" WHERE `").append(primaryKey).append("` = ");
		if (primaryKeyValue instanceof String) {
			query.append("'").append(primaryKeyValue.toString().replace("'", "\\'")).append("'");
		} else {
			query.append(primaryKeyValue);
		}
		
		return query.toString();
	}
	
	private Map<String, Object> convertRecordToMap(SinkRecord record) {
		Map<String, Object> rowData = new HashMap<>();
		
		// Handle different value types
		Object value = record.value();
		if (value instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> mapValue = (Map<String, Object>) value;
			rowData.putAll(mapValue);
		} else if (value instanceof String) {
			// If it's a JSON string, you might want to parse it
			rowData.put("value", value);
		} else {
			// For other types, convert to string
			rowData.put("value", value != null ? value.toString() : null);
		}
		
		// Add metadata fields
		rowData.put("_kafka_topic", record.topic());
		rowData.put("_kafka_partition", record.kafkaPartition());
		rowData.put("_kafka_offset", record.kafkaOffset());
		rowData.put("_kafka_timestamp", record.timestamp());
		
		return rowData;
	}
}
