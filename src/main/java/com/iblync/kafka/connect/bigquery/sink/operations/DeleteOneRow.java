package com.iblync.kafka.connect.bigquery.sink.operations;

import java.util.Map;

import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;
import com.iblync.kafka.connect.bigquery.util.BigQueryClientManager;

/**
 * BigQuery DELETE operation implementation
 */
public class DeleteOneRow extends AbstractBigQueryOperation {

    @Override
    protected void executeOperation(TopicConfiguration topicConfig, CDCMessage cdcMessage) throws Exception {
        // For DELETE: use 'before' data (after should be null)
        Map<String, Object> beforeData = cdcMessage.getBefore();
        
        if (beforeData == null || beforeData.isEmpty()) {
            throw new RuntimeException("DELETE operation requires 'before' data");
        }
        
        // Get primary key from before data
        Object primaryKeyValue = beforeData.get(topicConfig.primaryKey);
        if (primaryKeyValue == null) {
            throw new RuntimeException("Primary key '" + topicConfig.primaryKey + "' not found in before record");
        }
        
        // Debug logging
        logger.info("DEBUG: Executing DELETE operation");
        logger.info("DEBUG: Topic config - table: {}, dataset: {}, primaryKey: {}",
            topicConfig.tableName, topicConfig.datasetName, topicConfig.primaryKey);
        logger.info("DEBUG: CDC message: {}", cdcMessage);
        logger.info("DEBUG: Primary key value: {}", primaryKeyValue);
        System.out.println("DEBUG: Primary key value: " + primaryKeyValue);
        
        // Build and execute DELETE query
        String deleteQuery = buildDeleteQuery(topicConfig, primaryKeyValue);
        BigQueryClientManager.executeQuery(bigQuery, deleteQuery);
        
        logger.info("Deleted record with {} = {}", topicConfig.primaryKey, primaryKeyValue);
    }

    @Override
    protected String getOperationName() {
        return "DELETE";
    }
    
    /**
     * Builds a DELETE SQL query using primary key value
     */
    private String buildDeleteQuery(TopicConfiguration topicConfig, Object primaryKeyValue) {
        StringBuilder query = new StringBuilder();
        String tableName = BigQueryClientManager.buildTableName(config.project(), topicConfig.datasetName, topicConfig.tableName);
        
        query.append("DELETE FROM ").append(tableName)
             .append(" WHERE `").append(topicConfig.primaryKey).append("` = ")
             .append(BigQueryClientManager.escapeValue(primaryKeyValue));
        
        logger.debug("Generated DELETE query: {}", query.toString());
        return query.toString();
    }
}
