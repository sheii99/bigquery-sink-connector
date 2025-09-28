package com.iblync.kafka.connect.bigquery.sink.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.iblync.kafka.connect.bigquery.sink.ConcurrencyHint;
import com.iblync.kafka.connect.bigquery.sink.SinkAction;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.sink.handler.BigQueryHandler;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerContext;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerParams;
import com.iblync.kafka.connect.bigquery.util.TopicMap;

import reactor.core.publisher.Mono;

public class InsertOneRow implements BigQueryHandler, Closeable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertOneRow.class);
    private BigQuery bigQuery;
    private BigQueryConfig config;
    private Map<String, String> topicToTableMap;
    private Map<String, String> topicToDatasetMap;

    @Override
    public void init(SinkHandlerContext context, BigQueryConfig config) {
        this.config = config;
        Map<String, String> props = context.configProperties();
        
        // Parse topic-to-table and topic-to-dataset mappings using TopicMap utility
        this.topicToTableMap = TopicMap.parseTopicToValues(config.topicToTable(), null);
        this.topicToDatasetMap = TopicMap.parseTopicToValues(config.topicToDataset(), config.dataset());
        
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
        
        LOGGER.info("InsertOneRow initialized for project: {}, topicToTable: {}, topicToDataset: {}", 
                config.project(), topicToTableMap, topicToDatasetMap);
    }

    @Override
    public SinkAction handle(SinkHandlerParams params) {
        SinkRecord record = params.sinkRecord();
        String topic = record.topic();
        
        // Get table and dataset for this topic
        String tableName = topicToTableMap.get(topic);
        String datasetName = topicToDatasetMap.getOrDefault(topic, config.dataset());
        
        if (tableName == null) {
            LOGGER.warn("No table mapping found for topic: {}", topic);
            return SinkAction.ignore();
        }
        	
        return new SinkAction(
            Mono.fromCallable(() -> {
                try {
                    // Convert record value to Map
                    Map<String, Object> rowData = convertRecordToMap(record);
                    
                    // Ensure dataset and table exist
                    TableId table = TableId.of(config.project(), datasetName, tableName);
                    ensureDatasetExists(datasetName);
                    ensureTableExists(table, rowData);
                    
                    // Create insert request
                    InsertAllRequest insertRequest = InsertAllRequest.newBuilder(table)
                            .addRow(rowData)
                            .build();
                    
                    // Execute insert
                    InsertAllResponse response = bigQuery.insertAll(insertRequest);
                    
                    if (response.hasErrors()) {
                        LOGGER.error("BigQuery insert failed for topic {}: {}", topic, response.getInsertErrors());
                        throw new RuntimeException("BigQuery insert failed: " + response.getInsertErrors());
                    }
                    
                    LOGGER.debug("Successfully inserted record to BigQuery table: {}.{}.{}",
                            config.project(), datasetName, tableName);
                    return null;
                    
                } catch (Exception e) {
                    LOGGER.error("Error inserting record to BigQuery for topic: {}", topic, e);
                    throw new RuntimeException("Error inserting record to BigQuery", e);
                }
            }),
            ConcurrencyHint.of(record.topic() + "-" + record.kafkaPartition())
        );
    }

    @Override
    public void close() throws IOException {
        if (bigQuery != null) {
            // BigQuery client doesn't need explicit closing
            LOGGER.info("InsertOneRow closed");
        }
    }
    
    private void ensureDatasetExists(String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(config.project(), datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            
            if (dataset == null) {
                LOGGER.info("Creating dataset: {}.{}", config.project(), datasetName);
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId)
                        .setDescription("Auto-created dataset for Kafka Connect BigQuery Sink")
                        .build();
                bigQuery.create(datasetInfo);
                LOGGER.info("Successfully created dataset: {}.{}", config.project(), datasetName);
            }
        } catch (BigQueryException e) {
            LOGGER.error("Error ensuring dataset exists: {}.{}", config.project(), datasetName, e);
            throw new RuntimeException("Error ensuring dataset exists", e);
        }
    }
    
    private void ensureTableExists(TableId tableId, Map<String, Object> sampleData) {
        try {
            Table table = bigQuery.getTable(tableId);
            
            if (table == null) {
                LOGGER.info("Creating table: {}.{}.{}", tableId.getProject(), tableId.getDataset(), tableId.getTable());
                
                // Create schema based on sample data
                Schema schema = createSchemaFromData(sampleData);
                
                TableDefinition tableDefinition = StandardTableDefinition.of(schema);
                TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition)
                        .setDescription("Auto-created table for Kafka Connect BigQuery Sink")
                        .build();
                
                bigQuery.create(tableInfo);
                LOGGER.info("Successfully created table: {}.{}.{}", tableId.getProject(), tableId.getDataset(), tableId.getTable());
            }
        } catch (BigQueryException e) {
            LOGGER.error("Error ensuring table exists: {}.{}.{}", tableId.getProject(), tableId.getDataset(), tableId.getTable(), e);
            throw new RuntimeException("Error ensuring table exists", e);
        }
    }
    
    private Schema createSchemaFromData(Map<String, Object> data) {
        Field.Builder[] fields = new Field.Builder[data.size()];
        int i = 0;
        
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            StandardSQLTypeName fieldType = inferFieldType(value);
            
            fields[i] = Field.newBuilder(fieldName, fieldType)
                    .setMode(Field.Mode.NULLABLE)
                    .build()
                    .toBuilder();
            i++;
        }
        
        Field[] fieldArray = new Field[fields.length];
        for (int j = 0; j < fields.length; j++) {
            fieldArray[j] = fields[j].build();
        }
        
        return Schema.of(fieldArray);
    }
    
    private StandardSQLTypeName inferFieldType(Object value) {
        if (value == null) {
            return StandardSQLTypeName.STRING; // Default to STRING for null values
        } else if (value instanceof String) {
            return StandardSQLTypeName.STRING;
        } else if (value instanceof Integer) {
            return StandardSQLTypeName.INT64;
        } else if (value instanceof Long) {
            return StandardSQLTypeName.INT64;
        } else if (value instanceof Float || value instanceof Double) {
            return StandardSQLTypeName.FLOAT64;
        } else if (value instanceof Boolean) {
            return StandardSQLTypeName.BOOL;
        } else {
            return StandardSQLTypeName.STRING; // Default to STRING for unknown types
        }
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
