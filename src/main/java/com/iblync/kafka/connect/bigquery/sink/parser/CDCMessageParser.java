package com.iblync.kafka.connect.bigquery.sink.parser;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iblync.kafka.connect.bigquery.sink.converter.SinkConverter;

/**
 * Parser for CDC (Change Data Capture) messages from Kafka records.
 * Extracts before, after, and operation type from CDC formatted messages.
 */
public class CDCMessageParser {
    
    private static final Logger logger = LoggerFactory.getLogger(CDCMessageParser.class);
    private static final SinkConverter sinkConverter = new SinkConverter();
    
    /**
     * Parses a SinkRecord to extract CDC message components
     * 
     * @param record The Kafka SinkRecord containing CDC data
     * @return CDCMessage containing before, after, and operation data
     * @throws IllegalArgumentException if the record doesn't contain valid CDC data
     */
    public static CDCMessage parse(SinkRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("SinkRecord cannot be null");
        }
        
        try {
            // Extract message data using existing SinkConverter
            Map<String, Object> messageData = extractMessageData(record);
            
            // Extract CDC components
            @SuppressWarnings("unchecked")
            Map<String, Object> before = (Map<String, Object>) messageData.get("before");
            
            @SuppressWarnings("unchecked")
            Map<String, Object> after = (Map<String, Object>) messageData.get("after");
            
            String op = (String) messageData.get("op");
            
            // Create CDC message
            CDCMessage cdcMessage = new CDCMessage(before, after, op);
            
            // Validate the message
            validateCDCMessage(cdcMessage);
            
            logger.debug("Parsed CDC message: op={}, before={}, after={}", 
                op, before != null, after != null);
            
            return cdcMessage;
            
        } catch (Exception e) {
            logger.error("Failed to parse CDC message from record: topic={}, partition={}, offset={}", 
                record.topic(), record.kafkaPartition(), record.kafkaOffset(), e);
            throw new RuntimeException("Failed to parse CDC message from record", e);
        }
    }
    
    /**
     * Extracts message data from SinkRecord using existing SinkConverter
     */
    private static Map<String, Object> extractMessageData(SinkRecord record) {
        Map<String, Object> messageData = sinkConverter.convert(record);
        
        if (messageData == null || messageData.isEmpty()) {
            throw new IllegalArgumentException("Record contains no data");
        }
        
        return messageData;
    }
    
    /**
     * Validates CDC message structure and data consistency
     */
    private static void validateCDCMessage(CDCMessage message) {
        String op = message.getOp();
        
        // Validate operation type
        if (op == null || op.trim().isEmpty()) {
            throw new IllegalArgumentException("CDC message missing 'op' field");
        }
        
        String normalizedOp = op.toLowerCase().trim();
        if (!normalizedOp.equals("c") && !normalizedOp.equals("i") && 
            !normalizedOp.equals("u") && !normalizedOp.equals("d")) {
            throw new IllegalArgumentException("Invalid CDC operation: " + op + 
                ". Expected: c (create), i (insert), u (update), or d (delete)");
        }
        
        // Validate data consistency based on operation type
        Map<String, Object> before = message.getBefore();
        Map<String, Object> after = message.getAfter();
        
        switch (normalizedOp) {
            case "c":
            case "i":
                // INSERT: after should not be null, before should be null
                if (after == null || after.isEmpty()) {
                    throw new IllegalArgumentException("INSERT operation requires 'after' data");
                }
                if (before != null && !before.isEmpty()) {
                    logger.warn("INSERT operation has 'before' data, which is unusual but allowed");
                }
                break;
                
            case "u":
                // UPDATE: both before and after should not be null
                if (before == null || before.isEmpty()) {
                    throw new IllegalArgumentException("UPDATE operation requires 'before' data");
                }
                if (after == null || after.isEmpty()) {
                    throw new IllegalArgumentException("UPDATE operation requires 'after' data");
                }
                break;
                
            case "d":
                // DELETE: before should not be null, after should be null
                if (before == null || before.isEmpty()) {
                    throw new IllegalArgumentException("DELETE operation requires 'before' data");
                }
                if (after != null && !after.isEmpty()) {
                    logger.warn("DELETE operation has 'after' data, which is unusual but allowed");
                }
                break;
        }
    }
    
    /**
     * Immutable data holder for CDC message components
     */
    public static class CDCMessage {
        private final Map<String, Object> before;
        private final Map<String, Object> after;
        private final String op;
        
        /**
         * Creates a new CDC message
         * 
         * @param before The record state before the change (can be null for INSERT)
         * @param after The record state after the change (can be null for DELETE)
         * @param op The operation type (c/i for insert, u for update, d for delete)
         */
        public CDCMessage(Map<String, Object> before, Map<String, Object> after, String op) {
            this.before = before;
            this.after = after;
            this.op = op;
        }
        
        /**
         * @return The record state before the change (null for INSERT operations)
         */
        public Map<String, Object> getBefore() {
            return before;
        }
        
        /**
         * @return The record state after the change (null for DELETE operations)
         */
        public Map<String, Object> getAfter() {
            return after;
        }
        
        /**
         * @return The operation type (c/i for insert, u for update, d for delete)
         */
        public String getOp() {
            return op;
        }
        
        /**
         * @return true if this is an INSERT operation (c or i)
         */
        public boolean isInsert() {
            if (op == null) return false;
            String normalizedOp = op.toLowerCase().trim();
            return normalizedOp.equals("c") || normalizedOp.equals("i");
        }
        
        /**
         * @return true if this is an UPDATE operation (u)
         */
        public boolean isUpdate() {
            if (op == null) return false;
            return op.toLowerCase().trim().equals("u");
        }
        
        /**
         * @return true if this is a DELETE operation (d)
         */
        public boolean isDelete() {
            if (op == null) return false;
            return op.toLowerCase().trim().equals("d");
        }
        
        @Override
        public String toString() {
            return String.format("CDCMessage{op='%s', before=%s, after=%s}", 
                op, 
                before != null ? "present" : "null", 
                after != null ? "present" : "null");
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CDCMessage that = (CDCMessage) o;
            return Objects.equals(before, that.before) &&
                   Objects.equals(after, that.after) &&
                   Objects.equals(op, that.op);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(before, after, op);
        }
    }
}