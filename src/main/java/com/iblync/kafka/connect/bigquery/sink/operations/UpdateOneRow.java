package com.iblync.kafka.connect.bigquery.sink.operations;

import java.util.Map;

import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;
import com.iblync.kafka.connect.bigquery.sink.util.FieldComparator;
import com.iblync.kafka.connect.bigquery.util.BigQueryClientManager;

/**
 * BigQuery UPDATE operation implementation
 */
public class UpdateOneRow extends AbstractBigQueryOperation {

    @Override
    protected void executeOperation(TopicConfiguration topicConfig, CDCMessage cdcMessage) throws Exception {
        // For UPDATE: compare 'before' and 'after' data
        Map<String, Object> beforeData = cdcMessage.getBefore();
        Map<String, Object> afterData = cdcMessage.getAfter();
        
        if (beforeData == null || beforeData.isEmpty()) {
            throw new RuntimeException("UPDATE operation requires 'before' data");
        }
        if (afterData == null || afterData.isEmpty()) {
            throw new RuntimeException("UPDATE operation requires 'after' data");
        }
        
        // Get only changed fields
        Map<String, Object> changedFields = FieldComparator.getChangedFields(beforeData, afterData);
        
        if (changedFields.isEmpty()) {
            logger.info("No fields changed for record, skipping update");
            System.out.println("No fields changed for record, skipping update");
            return;
        }
        
        // Get primary key from before data
        Object primaryKeyValue = beforeData.get(topicConfig.primaryKey);
        if (primaryKeyValue == null) {
            throw new RuntimeException("Primary key '" + topicConfig.primaryKey + "' not found in before record");
        }
        
        // Debug logging
        logger.info("DEBUG: Executing UPDATE operation");
        logger.info("DEBUG: Topic config - table: {}, dataset: {}, primaryKey: {}",
            topicConfig.tableName, topicConfig.datasetName, topicConfig.primaryKey);
        logger.info("DEBUG: CDC message: {}", cdcMessage);
        logger.info("DEBUG: Changed fields: {}", changedFields);
        logger.info("DEBUG: Primary key value: {}", primaryKeyValue);
        System.out.println("DEBUG: Changed fields: " + changedFields);
        System.out.println("DEBUG: Primary key value: " + primaryKeyValue);
        
        // Build and execute optimized UPDATE query with only changed fields
        String updateQuery = buildUpdateQueryForChangedFields(topicConfig, changedFields, primaryKeyValue);
        BigQueryClientManager.executeQuery(bigQuery, updateQuery);
        
        logger.info("Updated {} fields for record with {} = {}",
            changedFields.size(), topicConfig.primaryKey, primaryKeyValue);
    }

    @Override
    protected String getOperationName() {
        return "UPDATE";
    }


    /**
     * Builds an optimized UPDATE SQL query with only changed fields
     */
    private String buildUpdateQueryForChangedFields(TopicConfiguration topicConfig,
                                                   Map<String, Object> changedFields,
                                                   Object primaryKeyValue) {
        StringBuilder query = new StringBuilder();
        String tableName = BigQueryClientManager.buildTableName(config.project(), topicConfig.datasetName, topicConfig.tableName);
        
        query.append("UPDATE ").append(tableName).append(" SET ");
        
        // Only include changed fields in SET clause
        boolean first = true;
        for (Map.Entry<String, Object> entry : changedFields.entrySet()) {
            String column = entry.getKey();
            Object value = entry.getValue();
            
            // Skip primary key in SET clause (shouldn't be in changed fields anyway)
            if (column.equals(topicConfig.primaryKey)) {
                continue;
            }
            
            if (!first) {
                query.append(", ");
            }
            
            query.append("`").append(column).append("` = ")
                 .append(BigQueryClientManager.escapeValue(value));
            first = false;
        }
        
        // WHERE clause uses primary key from before record
        query.append(" WHERE `").append(topicConfig.primaryKey).append("` = ")
             .append(BigQueryClientManager.escapeValue(primaryKeyValue));
        
        logger.debug("Generated UPDATE query: {}", query.toString());
        return query.toString();
    }
}
