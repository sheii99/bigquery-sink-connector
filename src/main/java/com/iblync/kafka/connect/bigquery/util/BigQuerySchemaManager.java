package com.iblync.kafka.connect.bigquery.util;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

/**
 * Utility class for BigQuery schema and table management
 */
public class BigQuerySchemaManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQuerySchemaManager.class);
    
    /**
     * Ensures that a dataset exists, creating it if necessary
     */
    public static void ensureDatasetExists(BigQuery bigQuery, String projectId, String datasetName) {
        try {
            DatasetId datasetId = DatasetId.of(projectId, datasetName);
            Dataset dataset = bigQuery.getDataset(datasetId);
            
            if (dataset == null) {
                LOGGER.info("Creating dataset: {}.{}", projectId, datasetName);
                DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetId)
                        .setDescription("Auto-created dataset for Kafka Connect BigQuery Sink")
                        .build();
                bigQuery.create(datasetInfo);
                LOGGER.info("Successfully created dataset: {}.{}", projectId, datasetName);
            }
        } catch (BigQueryException e) {
            LOGGER.error("Error ensuring dataset exists: {}.{}", projectId, datasetName, e);
            throw new RuntimeException("Error ensuring dataset exists", e);
        }
    }
    
    /**
     * Ensures that a table exists, creating it if necessary with schema inferred from sample data
     */
    public static void ensureTableExists(BigQuery bigQuery, TableId tableId, Map<String, Object> sampleData) {
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
    
    /**
     * Creates a BigQuery schema from sample data
     */
    public static Schema createSchemaFromData(Map<String, Object> data) {
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
    
    /**
     * Infers BigQuery field type from a Java object
     */
    public static StandardSQLTypeName inferFieldType(Object value) {
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
}