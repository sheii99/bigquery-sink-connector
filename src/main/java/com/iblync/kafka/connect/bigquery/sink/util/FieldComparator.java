package com.iblync.kafka.connect.bigquery.sink.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for comparing before and after records in CDC operations.
 * Provides methods to identify changed fields for efficient UPDATE operations.
 */
public class FieldComparator {
    
    private static final Logger logger = LoggerFactory.getLogger(FieldComparator.class);
    
    /**
     * Compares before and after records and returns only the fields that have changed.
     * This enables efficient UPDATE operations that only modify changed fields.
     * 
     * @param before The record state before the change
     * @param after The record state after the change
     * @return Map containing only the fields that changed, with their new values
     * @throws IllegalArgumentException if either before or after is null
     */
    public static Map<String, Object> getChangedFields(Map<String, Object> before, Map<String, Object> after) {
        if (before == null) {
            throw new IllegalArgumentException("Before record cannot be null for field comparison");
        }
        if (after == null) {
            throw new IllegalArgumentException("After record cannot be null for field comparison");
        }
        
        Map<String, Object> changedFields = new HashMap<>();
        
        // Check each field in the after record
        for (Map.Entry<String, Object> entry : after.entrySet()) {
            String fieldName = entry.getKey();
            Object afterValue = entry.getValue();
            Object beforeValue = before.get(fieldName);
            
            // Check if field value changed
            if (!Objects.equals(beforeValue, afterValue)) {
                changedFields.put(fieldName, afterValue);
                
                logger.debug("Field '{}' changed from '{}' to '{}'", 
                    fieldName, beforeValue, afterValue);
            }
        }
        
        // Handle new fields (exist in after but not in before)
        for (String fieldName : after.keySet()) {
            if (!before.containsKey(fieldName)) {
                changedFields.put(fieldName, after.get(fieldName));
                
                logger.debug("New field '{}' added with value '{}'", 
                    fieldName, after.get(fieldName));
            }
        }
        
        // Log removed fields (exist in before but not in after) - for information only
        for (String fieldName : before.keySet()) {
            if (!after.containsKey(fieldName)) {
                logger.debug("Field '{}' removed (was '{}')", 
                    fieldName, before.get(fieldName));
            }
        }
        
        logger.debug("Field comparison complete: {} out of {} fields changed", 
            changedFields.size(), after.size());
        
        return changedFields;
    }
    
    /**
     * Checks if any fields have changed between before and after records.
     * This is a more efficient way to check if an update is needed without
     * creating the full changed fields map.
     * 
     * @param before The record state before the change
     * @param after The record state after the change
     * @return true if any fields have changed, false otherwise
     */
    public static boolean hasChanges(Map<String, Object> before, Map<String, Object> after) {
        if (before == null || after == null) {
            return true; // Consider null changes as having changes
        }
        
        // Quick size check
        if (before.size() != after.size()) {
            return true;
        }
        
        // Check each field in after record
        for (Map.Entry<String, Object> entry : after.entrySet()) {
            String fieldName = entry.getKey();
            Object afterValue = entry.getValue();
            Object beforeValue = before.get(fieldName);
            
            if (!Objects.equals(beforeValue, afterValue)) {
                return true;
            }
        }
        
        // Check for removed fields
        for (String fieldName : before.keySet()) {
            if (!after.containsKey(fieldName)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Gets a summary of changes between before and after records.
     * Useful for logging and debugging purposes.
     * 
     * @param before The record state before the change
     * @param after The record state after the change
     * @return A human-readable summary of the changes
     */
    public static String getChangesSummary(Map<String, Object> before, Map<String, Object> after) {
        if (before == null || after == null) {
            return "Cannot compare null records";
        }
        
        Map<String, Object> changedFields = getChangedFields(before, after);
        
        if (changedFields.isEmpty()) {
            return "No fields changed";
        }
        
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("%d field(s) changed: ", changedFields.size()));
        
        boolean first = true;
        for (String fieldName : changedFields.keySet()) {
            if (!first) {
                summary.append(", ");
            }
            summary.append(fieldName);
            first = false;
        }
        
        return summary.toString();
    }
}