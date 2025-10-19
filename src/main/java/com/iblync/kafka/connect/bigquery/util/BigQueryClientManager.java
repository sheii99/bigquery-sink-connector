package com.iblync.kafka.connect.bigquery.util;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;

/**
 * Manages BigQuery client creation and common BigQuery operations
 */
public class BigQueryClientManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BigQueryClientManager.class);
    
    /**
     * Creates a BigQuery client instance
     */
    public static BigQuery createBigQueryClient(BigQueryConfig config, Map<String, String> props) {
        String keyFileContent = props.get("bigquery.keyfile");
        try {
            if (keyFileContent != null && !keyFileContent.isEmpty()) {
                // Check if it's a JSON string (starts with '{') or a file path
                if (keyFileContent.trim().startsWith("{")) {
                    // It's a JSON string - parse directly
                    return BigQueryOptions.newBuilder()
                            .setProjectId(config.project())
                            .setCredentials(com.google.auth.oauth2.ServiceAccountCredentials.fromStream(
                                    new java.io.ByteArrayInputStream(keyFileContent.getBytes(java.nio.charset.StandardCharsets.UTF_8))))
                            .build()
                            .getService();
                } else {
                    // It's a file path - load as resource
                    return BigQueryOptions.newBuilder()
                            .setProjectId(config.project())
                            .setCredentials(com.google.auth.oauth2.ServiceAccountCredentials.fromStream(
                                    BigQueryClientManager.class.getResourceAsStream(keyFileContent)))
                            .build()
                            .getService();
                }
            } else {
                // Use default credentials
                return BigQueryOptions.newBuilder()
                        .setProjectId(config.project())
                        .build()
                        .getService();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to initialize BigQuery client", e);
            throw new RuntimeException("Failed to initialize BigQuery client", e);
        }
    }
    
    /**
     * Executes a SQL query and waits for completion
     */
    public static void executeQuery(BigQuery bigQuery, String query) throws InterruptedException {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
                .setUseLegacySql(false)
                .build();
        
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
        

        /*
         * 
         *  JobId jobId = JobId.newBuilder().setProject(projectId).build();
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
         */

         
        // Wait for the query to complete
        queryJob = queryJob.waitFor();
        
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException("BigQuery query failed: " + queryJob.getStatus().getError());
        }
    }
    
    /**
     * Escapes a value for use in SQL queries
     */
    public static String escapeValue(Object value) {
        if (value == null) {
            return "NULL";
        } else if (value instanceof String) {
            return "'" + value.toString().replace("'", "\\'") + "'";
        } else if (value instanceof Number) {
            return value.toString();
        } else {
            return "'" + value.toString().replace("'", "\\'") + "'";
        }
    }
    
    /**
     * Validates that a primary key exists in the data
     */
    public static void validatePrimaryKey(Map<String, Object> data, String primaryKey) {
        Object primaryKeyValue = data.get(primaryKey);
        if (primaryKeyValue == null) {
            throw new RuntimeException("Primary key '" + primaryKey + "' not found in record data");
        }
    }
    
    /**
     * Builds a fully qualified table name
     */
    public static String buildTableName(String projectId, String datasetName, String tableName) {
        return "`" + projectId + "." + datasetName + "." + tableName + "`";
    }
}