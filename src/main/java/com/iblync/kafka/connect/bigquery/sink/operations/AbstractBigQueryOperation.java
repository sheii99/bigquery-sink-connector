package com.iblync.kafka.connect.bigquery.sink.operations;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;
import com.iblync.kafka.connect.bigquery.sink.ConcurrencyHint;
import com.iblync.kafka.connect.bigquery.sink.SinkAction;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.sink.converter.SinkConverter;
import com.iblync.kafka.connect.bigquery.sink.handler.BigQueryHandler;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerContext;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerParams;
import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;
import com.iblync.kafka.connect.bigquery.util.BigQueryClientManager;
import com.iblync.kafka.connect.bigquery.util.TopicMap;

import reactor.core.publisher.Mono;

/**
 * Abstract base class for BigQuery operations that provides common functionality
 */
public abstract class AbstractBigQueryOperation implements BigQueryHandler, Closeable {
    
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected BigQuery bigQuery;
    protected BigQueryConfig config;
    protected Map<String, String> topicToTableMap;
    protected Map<String, String> topicToDatasetMap;
    protected Map<String, String> topicToPkMap;
    protected SinkConverter sinkConverter;
    
    @Override
    public void init(SinkHandlerContext context, BigQueryConfig config) {
        this.config = config;
        this.sinkConverter = new SinkConverter();
        Map<String, String> props = context.configProperties();
        
        // Parse topic mappings using TopicMap utility
        this.topicToTableMap = TopicMap.parseTopicToValues(config.topicToTable(), null);
        this.topicToDatasetMap = TopicMap.parseTopicToValues(config.topicToDataset(), config.dataset());
        this.topicToPkMap = TopicMap.parseTopicToValues(config.topicToPk(), "id");
        
        // Initialize BigQuery client using utility
        this.bigQuery = BigQueryClientManager.createBigQueryClient(config, props);
        
        logger.info("{} initialized for project: {}, topicToTable: {}, topicToDataset: {}, topicToPk: {}", 
                getClass().getSimpleName(), config.project(), topicToTableMap, topicToDatasetMap, topicToPkMap);
    
        System.out.println(getClass().getSimpleName() + 
        	    " initialized for project: " + config.project() +
        	    ", topicToTable: " + topicToTableMap +
        	    ", topicToDataset: " + topicToDatasetMap +
        	    ", topicToPk: " + topicToPkMap);
    
    }
    
    @Override
    public SinkAction handle(SinkHandlerParams params) {
        SinkRecord record = params.sinkRecord();
        CDCMessage cdcMessage = params.getCdcMessage();
        String topic = record.topic();
        
        // Validate CDC message is present
        if (cdcMessage == null) {
            throw new RuntimeException("CDC message is required for operation processing");
        }
        
        // Extract topic configuration
        TopicConfiguration topicConfig = extractTopicConfiguration(topic);
        if (topicConfig.tableName == null) {
            logger.warn("No table mapping found for topic: {}", topic);
            System.out.println("WARNING: No table mapping found for topic: " + topic);
            return SinkAction.ignore();
        }
        
        return new SinkAction(
            Mono.fromCallable(() -> {
                try {
                    // Debug logging
                    logger.info("DEBUG: AbstractBigQueryOperation - Processing CDC message: {}", cdcMessage);
                    logger.info("DEBUG: AbstractBigQueryOperation - Operation: {}, Topic: {}, Table: {}.{}.{}",
                        getOperationName(), topic, config.project(), topicConfig.datasetName, topicConfig.tableName);
                    System.out.println("DEBUG: AbstractBigQueryOperation - Processing CDC message: " + cdcMessage);
                    
                    // Execute operation-specific logic with CDC data
                    executeOperation(topicConfig, cdcMessage);
                    
                    logger.debug("Successfully executed {} operation for topic: {} on table: {}.{}.{}",
                            getOperationName(), topic, config.project(), topicConfig.datasetName, topicConfig.tableName);
                    return null;
                    
                } catch (Exception e) {
                    logger.error("Error executing {} operation for topic: {}", getOperationName(), topic, e);
                    throw new RuntimeException("Error executing " + getOperationName() + " operation", e);
                }
            }),
            ConcurrencyHint.of(record.topic() + "-" + record.kafkaPartition())
        );
    }
    
    @Override
    public void close() throws IOException {
        if (bigQuery != null) {
            logger.info("{} closed", getClass().getSimpleName());
        }
    }
    
    /**
     * Extracts topic-specific configuration
     */
    protected TopicConfiguration extractTopicConfiguration(String topic) {
        String tableName = topicToTableMap.get(topic);
        String datasetName = topicToDatasetMap.getOrDefault(topic, config.dataset());
        String primaryKey = topicToPkMap.getOrDefault(topic, "id");
        
        return new TopicConfiguration(tableName, datasetName, primaryKey);
    }
    
    /**
     * Template method for operation-specific logic with CDC data
     */
    protected abstract void executeOperation(TopicConfiguration topicConfig, CDCMessage cdcMessage) throws Exception;
    
    /**
     * Returns the name of the operation for logging purposes
     */
    protected abstract String getOperationName();
    
    /**
     * Inner class to hold topic-specific configuration
     */
    protected static class TopicConfiguration {
        public final String tableName;
        public final String datasetName;
        public final String primaryKey;
        
        public TopicConfiguration(String tableName, String datasetName, String primaryKey) {
            this.tableName = tableName;
            this.datasetName = datasetName;
            this.primaryKey = primaryKey;
        }
    }
}