# BigQuery Operations Analysis

## Overview

This document analyzes the three BigQuery operation classes in the `src/main/java/com/iblync/kafka/connect/bigquery/sink/operations` folder:

- `InsertOneRow.java` - Handles INSERT operations
- `UpdateOneRow.java` - Handles UPDATE operations  
- `DeleteOneRow.java` - Handles DELETE operations

## Class Logic Analysis

### 1. InsertOneRow Class

**Purpose**: Inserts new records into BigQuery tables with auto-creation of datasets and tables.

**Key Logic Flow**:
1. **Initialization**: Parse topic mappings, initialize BigQuery client
2. **Handle Method**: 
   - Extract topic, table, and dataset information
   - Convert Kafka record to Map
   - Ensure dataset and table exist (auto-create if needed)
   - Execute BigQuery insert operation
   - Return reactive SinkAction

**Unique Features**:
- Auto-creates datasets and tables if they don't exist
- Infers schema from sample data
- Uses BigQuery's `insertAll()` API for direct insertion

### 2. UpdateOneRow Class

**Purpose**: Updates existing records in BigQuery tables using SQL UPDATE statements.

**Key Logic Flow**:
1. **Initialization**: Parse topic mappings including primary key mappings
2. **Handle Method**:
   - Extract topic, table, dataset, and primary key information
   - Convert Kafka record to Map
   - Build dynamic UPDATE SQL query
   - Execute query via BigQuery job
   - Return reactive SinkAction

**Unique Features**:
- Builds dynamic SQL UPDATE statements
- Requires primary key for WHERE clause
- Uses BigQuery job execution for SQL operations

### 3. DeleteOneRow Class

**Purpose**: Deletes records from BigQuery tables using SQL DELETE statements.

**Key Logic Flow**:
1. **Initialization**: Parse topic mappings including primary key mappings
2. **Handle Method**:
   - Extract topic, table, dataset, and primary key information
   - Convert Kafka record to Map
   - Build DELETE SQL query using primary key
   - Execute query via BigQuery job
   - Return reactive SinkAction

**Unique Features**:
- Builds SQL DELETE statements
- Requires primary key for WHERE clause
- Uses BigQuery job execution for SQL operations

## Repeated Code Analysis

### 🔴 Major Code Duplication

#### 1. BigQuery Client Initialization (100% Duplicate)

**Location**: All three classes in `init()` method (lines 47-67)

```java
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
```

#### 2. Topic Mapping Parsing (95% Duplicate)

**Location**: All three classes in `init()` method

```java
// Parse topic-to-table, topic-to-dataset mappings
this.topicToTableMap = TopicMap.parseTopicToValues(config.topicToTable(), null);
this.topicToDatasetMap = TopicMap.parseTopicToValues(config.topicToDataset(), config.dataset());
// UpdateOneRow and DeleteOneRow also have:
this.topicToPkMap = TopicMap.parseTopicToValues(config.topicToPk(), "id");
```

#### 3. Record to Map Conversion (100% Duplicate)

**Location**: All three classes in `convertRecordToMap()` method (lines 155-179 in DeleteOneRow, 225-249 in InsertOneRow, 184-208 in UpdateOneRow)

```java
private Map<String, Object> convertRecordToMap(SinkRecord record) {
    Map<String, Object> rowData = new HashMap<>();
    
    // Handle different value types
    Object value = record.value();
    if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> mapValue = (Map<String, Object>) value;
        rowData.putAll(mapValue);
    } else if (value instanceof String) {
        rowData.put("value", value);
    } else {
        rowData.put("value", value != null ? value.toString() : null);
    }
    
    // Add metadata fields
    rowData.put("_kafka_topic", record.topic());
    rowData.put("_kafka_partition", record.kafkaPartition());
    rowData.put("_kafka_offset", record.kafkaOffset());
    rowData.put("_kafka_timestamp", record.timestamp());
    
    return rowData;
}
```

#### 4. Topic Configuration Extraction (90% Duplicate)

**Location**: All three classes in `handle()` method

```java
String topic = record.topic();
String tableName = topicToTableMap.get(topic);
String datasetName = topicToDatasetMap.getOrDefault(topic, config.dataset());
// UpdateOneRow and DeleteOneRow also have:
String primaryKey = topicToPkMap.getOrDefault(topic, "id");

if (tableName == null) {
    LOGGER.warn("No table mapping found for topic: {}", topic);
    return SinkAction.ignore();
}
```

#### 5. BigQuery Job Execution Pattern (90% Duplicate)

**Location**: UpdateOneRow and DeleteOneRow classes

```java
// Create and execute query job
QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .build();

JobId jobId = JobId.of(java.util.UUID.randomUUID().toString());
Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

// Wait for the query to complete
queryJob = queryJob.waitFor();

if (queryJob == null) {
    throw new RuntimeException("Job no longer exists");
} else if (queryJob.getStatus().getError() != null) {
    throw new RuntimeException("BigQuery operation failed: " + queryJob.getStatus().getError());
}
```

#### 6. Close Method (100% Duplicate)

**Location**: All three classes

```java
@Override
public void close() throws IOException {
    if (bigQuery != null) {
        LOGGER.info("ClassName closed");
    }
}
```

#### 7. SinkAction Creation Pattern (95% Duplicate)

**Location**: All three classes in `handle()` method

```java
return new SinkAction(
    Mono.fromCallable(() -> {
        try {
            // Operation-specific logic here
            return null;
        } catch (Exception e) {
            LOGGER.error("Error performing operation for topic: {}", topic, e);
            throw new RuntimeException("Error performing operation", e);
        }
    }),
    ConcurrencyHint.of(record.topic() + "-" + record.kafkaPartition())
);
```

### 🟡 Minor Code Duplication

#### 1. Field Declarations

All classes have similar field declarations:
```java
private static final Logger LOGGER = LoggerFactory.getLogger(ClassName.class);
private BigQuery bigQuery;
private BigQueryConfig config;
private Map<String, String> topicToTableMap;
private Map<String, String> topicToDatasetMap;
```

#### 2. SQL Value Escaping Logic

UpdateOneRow and DeleteOneRow have similar value escaping:
```java
if (value instanceof String) {
    query.append("'").append(value.toString().replace("'", "\\'")).append("'");
} else {
    query.append(value);
}
```

## Refactoring Recommendations

### 1. Create Abstract Base Class

Create `AbstractBigQueryOperation` with shared functionality:

```java
public abstract class AbstractBigQueryOperation implements BigQueryHandler, Closeable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(getClass());
    protected BigQuery bigQuery;
    protected BigQueryConfig config;
    protected Map<String, String> topicToTableMap;
    protected Map<String, String> topicToDatasetMap;
    protected Map<String, String> topicToPkMap;
    
    // Shared methods:
    // - initializeBigQueryClient()
    // - parseTopicMappings()
    // - convertRecordToMap()
    // - extractTopicConfiguration()
    // - createSinkAction()
    // - close()
}
```

### 2. Create BigQuery Utility Class

Create `BigQueryUtils` for common operations:

```java
public class BigQueryUtils {
    public static BigQuery createBigQueryClient(String projectId, String keyFilePath);
    public static Job executeQuery(BigQuery bigQuery, String query);
    public static String escapeValue(Object value);
    public static void validatePrimaryKey(Map<String, Object> data, String primaryKey);
}
```

### 3. Create Schema Management Utility

Extract schema creation logic from InsertOneRow:

```java
public class BigQuerySchemaManager {
    public static void ensureDatasetExists(BigQuery bigQuery, String projectId, String datasetName);
    public static void ensureTableExists(BigQuery bigQuery, TableId tableId, Map<String, Object> sampleData);
    public static Schema createSchemaFromData(Map<String, Object> data);
    public static StandardSQLTypeName inferFieldType(Object value);
}
```

### 4. Estimated Code Reduction

After refactoring:
- **InsertOneRow**: ~60% code reduction (150 → 60 lines)
- **UpdateOneRow**: ~70% code reduction (175 → 50 lines)  
- **DeleteOneRow**: ~70% code reduction (150 → 45 lines)
- **Total**: ~67% code reduction with better maintainability

### 5. Benefits of Refactoring

1. **Maintainability**: Single point of change for common logic
2. **Testability**: Easier to unit test shared components
3. **Consistency**: Uniform behavior across all operations
4. **Extensibility**: Easy to add new operation types
5. **Bug Fixes**: Fix once, apply everywhere
6. **Code Readability**: Focus on operation-specific logic

## Current Architecture Issues

1. **No Shared Error Handling**: Each class handles errors differently
2. **Inconsistent Logging**: Different log messages and levels
3. **Duplicate Configuration Parsing**: Same logic repeated 3 times
4. **No Connection Pooling**: Each class creates its own BigQuery client
5. **Hard to Test**: Tightly coupled initialization and business logic

## Recommended Next Steps

1. Create the abstract base class with shared functionality
2. Extract BigQuery client management to utility class
3. Move schema management to separate utility (InsertOneRow specific)
4. Refactor each operation class to extend the base class
5. Add comprehensive unit tests for the refactored components
6. Consider adding connection pooling and retry mechanisms

This refactoring would significantly improve code maintainability while preserving all existing functionality.