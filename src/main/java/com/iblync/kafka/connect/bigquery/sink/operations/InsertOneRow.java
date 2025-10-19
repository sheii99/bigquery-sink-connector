package com.iblync.kafka.connect.bigquery.sink.operations;

import java.util.Map;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;
import com.iblync.kafka.connect.bigquery.util.BigQuerySchemaManager;

/**
 * BigQuery INSERT operation implementation
 */
public class InsertOneRow extends AbstractBigQueryOperation {

    @Override
    protected void executeOperation(TopicConfiguration topicConfig, CDCMessage cdcMessage) throws Exception {
        // For INSERT: use 'after' data (before should be null)
        Map<String, Object> rowData = cdcMessage.getAfter();
        
        if (rowData == null || rowData.isEmpty()) {
            throw new RuntimeException("INSERT operation requires 'after' data");
        }
        
        // Debug logging
        logger.info("DEBUG: Executing INSERT operation");
        logger.info("DEBUG: Topic config - table: {}, dataset: {}", topicConfig.tableName, topicConfig.datasetName);
        logger.info("DEBUG: CDC message: {}", cdcMessage);
        logger.info("DEBUG: Row data from 'after': {}", rowData);
        System.out.println("DEBUG: Row data from 'after': " + rowData);
        
        // Ensure dataset and table exist (auto-create if needed)
        TableId tableId = TableId.of(config.project(), topicConfig.datasetName, topicConfig.tableName);
        logger.info("DEBUG: TableId created: {}", tableId);
        
        BigQuerySchemaManager.ensureDatasetExists(bigQuery, config.project(), topicConfig.datasetName);
        BigQuerySchemaManager.ensureTableExists(bigQuery, tableId, rowData);
        
        // Create and execute insert request
        InsertAllRequest insertRequest = InsertAllRequest.newBuilder(tableId)
                .addRow(rowData)
                .build();
        
        logger.info("DEBUG: About to execute insertAll with {} rows", insertRequest.getRows().size());
        InsertAllResponse response = bigQuery.insertAll(insertRequest);
        logger.info("DEBUG: InsertAll response received, hasErrors: {}", response.hasErrors());
        
        if (response.hasErrors()) {
            logger.error("BigQuery insert failed: {}", response.getInsertErrors());
            throw new RuntimeException("BigQuery insert failed: " + response.getInsertErrors());
        } else {
            logger.info("DEBUG: Insert completed successfully, no errors reported");
        }
    }

    @Override
    protected String getOperationName() {
        return "INSERT";
    }
}
