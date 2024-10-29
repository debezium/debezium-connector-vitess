/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageColumn;
import io.debezium.connector.vitess.connection.VitessTabletType;
import io.debezium.connector.vitess.pipeline.txmetadata.ShardEpochMap;
import io.debezium.embedded.EmbeddedEngineConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.history.MemorySchemaHistory;
import io.debezium.util.Testing;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;

import binlogdata.Binlogdata;

public class TestHelper {
    protected static final String TEST_SERVER = "test_server";
    public static final String TEST_UNSHARDED_KEYSPACE = "test_unsharded_keyspace";
    public static final String TEST_SHARDED_KEYSPACE = "test_sharded_keyspace";
    public static final String TEST_EMPTY_SHARD_KEYSPACE = "test_empty_shard_keyspace";
    public static final String TEST_NON_EMPTY_SHARD = "-80";
    public static final String TEST_SHARD = "0";
    public static final String TEST_SHARD1 = "-80";
    public static final String TEST_SHARD2 = "80-";
    public static final Long TEST_SHARD1_EPOCH = 2L;
    public static final Long TEST_SHARD2_EPOCH = 3L;
    public static final ShardEpochMap TEST_SHARD_TO_EPOCH = new ShardEpochMap(Map.of(TEST_SHARD1, TEST_SHARD1_EPOCH, TEST_SHARD2, TEST_SHARD2_EPOCH));

    public static final Path SCHEMA_HISTORY_PATH = Testing.Files.createTestingPath("file-schema-history-connect.txt").toAbsolutePath();

    public static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    public static final String TEST_TABLE = "test_table";
    private static final String TEST_VITESS_FULL_TABLE = TEST_UNSHARDED_KEYSPACE + "." + TEST_TABLE;
    protected static final String PK_FIELD = "id";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final String VTCTLD_HOST = "localhost";
    private static final int VTCTLD_PORT = 15999;
    private static final String VTGATE_HOST = "localhost";
    private static final int VTGATE_PORT = 15991;
    // Use the same username and password for vtgate and vtctld
    public static final String USERNAME = "vitess";
    public static final String PASSWORD = "vitess_password";

    protected static final String VGTID_JSON_NO_PKS_TEMPLATE = "[" +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}," +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}" +
            "]";

    public static final String VGTID_SINGLE_SHARD_JSON_TEMPLATE = "[" +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":[]}" +
            "]";

    public static final String VGTID_JSON_TEMPLATE = "[" +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":[]}," +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":[]}" +
            "]";

    protected static final String INSERT_STMT = "INSERT INTO t1 (int_col) VALUES (1);";
    protected static final List<String> SETUP_TABLES_STMT = Arrays.asList(
            "DROP TABLE IF EXISTS t1;",
            "CREATE TABLE t1 (id BIGINT NOT NULL AUTO_INCREMENT, int_col INT, PRIMARY KEY (id));");

    public static Configuration.Builder defaultConfig() {
        return defaultConfig(false, false, 1, -1, -1, null, VitessConnectorConfig.SnapshotMode.NEVER, TEST_SHARD, null, null);
    }

    /**
     * Get the default configuration of the connector
     *
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @return Configuration builder
     */
    public static Configuration.Builder defaultConfig(boolean hasMultipleShards,
                                                      boolean offsetStoragePerTask,
                                                      int numTasks,
                                                      int gen,
                                                      int prevNumTasks,
                                                      String tableInclude,
                                                      VitessConnectorConfig.SnapshotMode snapshotMode) {
        return defaultConfig(hasMultipleShards,
                offsetStoragePerTask,
                numTasks,
                gen,
                prevNumTasks,
                tableInclude,
                snapshotMode,
                "",
                null,
                null);
    }

    public static Configuration.Builder defaultConfig(boolean hasMultipleShards,
                                                      boolean offsetStoragePerTask,
                                                      int numTasks,
                                                      int gen,
                                                      int prevNumTasks,
                                                      String tableInclude,
                                                      VitessConnectorConfig.SnapshotMode snapshotMode,
                                                      String shards) {
        return defaultConfig(hasMultipleShards,
                offsetStoragePerTask,
                numTasks,
                gen,
                prevNumTasks,
                tableInclude,
                snapshotMode,
                shards,
                null,
                null);
    }

    public static Configuration.Builder defaultConfig(boolean hasMultipleShards,
                                                      boolean offsetStoragePerTask,
                                                      int numTasks,
                                                      int gen,
                                                      int prevNumTasks,
                                                      String tableInclude,
                                                      VitessConnectorConfig.SnapshotMode snapshotMode,
                                                      String shards,
                                                      String grpcMaxInboundMessageSize,
                                                      String eventProcessingFailureHandlingMode) {
        Configuration.Builder builder = Configuration.create();
        builder = builder
                .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                .with(VitessConnectorConfig.VTGATE_HOST, VTGATE_HOST)
                .with(VitessConnectorConfig.VTGATE_PORT, VTGATE_PORT)
                .with(VitessConnectorConfig.VTGATE_USER, USERNAME)
                .with(VitessConnectorConfig.VTGATE_PASSWORD, PASSWORD)
                .with(VitessConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(VitessConnectorConfig.SCHEMA_HISTORY, MemorySchemaHistory.class)
                .with(EmbeddedEngineConfig.WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_MS, 5000)
                .with(VitessConnectorConfig.POLL_INTERVAL_MS, 100);
        if (!Strings.isNullOrEmpty(tableInclude)) {
            builder.with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude);
        }
        if (hasMultipleShards) {
            builder = builder.with(VitessConnectorConfig.KEYSPACE, TEST_SHARDED_KEYSPACE);
        }
        else {
            builder = builder.with(VitessConnectorConfig.KEYSPACE, TEST_UNSHARDED_KEYSPACE);
        }
        if (shards != null && !shards.isEmpty()) {
            builder.with(VitessConnectorConfig.SHARD, shards);
        }

        if (offsetStoragePerTask) {
            builder = builder.with(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK, "true")
                    .with(VitessConnectorConfig.TASKS_MAX_CONFIG, Integer.toString(numTasks))
                    .with(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN, Integer.toString(gen))
                    .with(VitessConnectorConfig.PREV_NUM_TASKS, Integer.toString(prevNumTasks));
        }
        if (snapshotMode != null) {
            builder = builder.with(VitessConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue());
        }
        if (grpcMaxInboundMessageSize != null) {
            builder = builder.with(VitessConnectorConfig.GRPC_MAX_INBOUND_MESSAGE_SIZE, grpcMaxInboundMessageSize);
        }
        if (eventProcessingFailureHandlingMode != null) {
            builder = builder.with(VitessConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE, eventProcessingFailureHandlingMode);
        }
        return builder;
    }

    public static void execute(List<String> statements) {
        execute(statements, TEST_UNSHARDED_KEYSPACE);
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the connection
     *
     * @param statements A list of SQL statements
     * @param database   Keyspace
     */
    public static void execute(List<String> statements, String database) {

        try (MySQLConnection connection = MySQLConnection.forTestDatabase(database)) {
            connection.setAutoCommit(false);
            Connection jdbcConn = null;
            for (String statement : statements) {
                connection.executeWithoutCommitting(statement);
                jdbcConn = connection.connection();
            }
            jdbcConn.commit();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void execute(String statement) {
        execute(statement, TEST_UNSHARDED_KEYSPACE);
    }

    public static void execute(String statement, String database) {
        execute(Collections.singletonList(statement), database);
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        executeDDL(ddlFile, TEST_UNSHARDED_KEYSPACE);
    }

    public static void executeDDL(String ddlFile, VitessConnectorConfig config, String shard) throws IOException, URISyntaxException {
        String statements = readStringFromFile(ddlFile);
        for (String statement : Arrays.asList(statements.split(";"))) {
            new VitessMetadata(config).executeQuery(statement, shard);
        }
    }

    protected static void executeDDL(String ddlFile, String database) throws Exception {
        String statements = readStringFromFile(ddlFile);
        execute(Arrays.asList(statements.split(";")), database);
    }

    protected static void applyVSchema(String vschemaFile) throws Exception {
        try (VtctldConnection vtctldConnection = VtctldConnection.of(VTCTLD_HOST, VTCTLD_PORT, USERNAME, PASSWORD)) {
            vtctldConnection.applyVSchema(readStringFromFile(vschemaFile), TEST_SHARDED_KEYSPACE);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static String applyOnlineDdl(String ddl, String keyspace) {
        try (VtctldConnection vtctldConnection = VtctldConnection.of(VTCTLD_HOST, VTCTLD_PORT, USERNAME, PASSWORD)) {
            return vtctldConnection.applySchema(ddl, "vitess", keyspace);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static boolean checkOnlineDDL(String keyspace, String id) {
        try (VtctldConnection vtctldConnection = VtctldConnection.of(VTCTLD_HOST, VTCTLD_PORT, USERNAME, PASSWORD)) {
            return vtctldConnection.checkOnlineDdlCompleted(keyspace, id);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static Vgtid getCurrentVgtid() throws Exception {
        try (VtctldConnection vtctldConnection = VtctldConnection.of(VTCTLD_HOST, VTCTLD_PORT, USERNAME, PASSWORD)) {
            return vtctldConnection.latestVgtid(TEST_UNSHARDED_KEYSPACE, TEST_SHARD, VitessTabletType.MASTER);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static String readStringFromFile(String ddlFile) throws IOException, URISyntaxException {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                .filter(line -> !line.isEmpty())
                .collect(Collectors.joining(System.lineSeparator()));
        return statements;
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "60"));
    }

    public static Binlogdata.VEvent defaultFieldEvent() {
        return newFieldEvent(defaultColumnValues());
    }

    public static Binlogdata.VEvent defaultFieldEvent(String shard, String keyspace) {
        return newFieldEvent(defaultColumnValues(), shard, keyspace);
    }

    public static Binlogdata.VEvent newFieldEvent(List<ColumnValue> columnValues) {
        return newFieldEvent(columnValues, TEST_SHARD, TEST_UNSHARDED_KEYSPACE);
    }

    public static Binlogdata.VEvent newFieldEvent(List<ColumnValue> columnValues, String shard, String keyspace) {
        Binlogdata.FieldEvent.Builder fieldEventBuilder = Binlogdata.FieldEvent.newBuilder()
                .setTableName(getFullTableName(keyspace, TEST_TABLE));
        for (Field field : newFields(columnValues)) {
            fieldEventBuilder.addFields(field);
        }
        fieldEventBuilder.setShard(shard);
        fieldEventBuilder.setKeyspace(keyspace);

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.FIELD)
                .setFieldEvent(fieldEventBuilder.build())
                .setTimestamp(AnonymousValue.getLong())
                .build();
    }

    private static String getFullTableName(String keyspace, String table) {
        return keyspace + "." + table;
    }

    public static Binlogdata.VEvent defaultInsertEvent() {
        return newInsertEvent(defaultColumnValues(), TEST_SHARD, TEST_UNSHARDED_KEYSPACE);
    }

    public static Binlogdata.VEvent insertEvent(List<ColumnValue> columnValues) {
        return newInsertEvent(columnValues, TEST_SHARD, TEST_UNSHARDED_KEYSPACE);
    }

    public static Binlogdata.VEvent insertEvent(List<ColumnValue> columnValues, String shard, String keyspace) {
        return newInsertEvent(columnValues, shard, keyspace);
    }

    public static Binlogdata.VEvent newInsertEvent(List<ColumnValue> columnValues, String shard, String keyspace) {
        List<byte[]> rawValues = newRawValues(columnValues);
        Query.Row row = newRow(rawValues);

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.ROW)
                .setRowEvent(
                        Binlogdata.RowEvent.newBuilder()
                                .addRowChanges(Binlogdata.RowChange.newBuilder().setAfter(row).build())
                                .setTableName(getFullTableName(keyspace, TEST_TABLE))
                                .setShard(shard)
                                .build())
                .setTimestamp(AnonymousValue.getLong())
                .build();
    }

    public static Binlogdata.VEvent defaultDeleteEvent() {
        return newDeleteEvent(defaultColumnValues());
    }

    public static Binlogdata.VEvent newDeleteEvent(List<ColumnValue> columnValues) {
        List<byte[]> rawValues = newRawValues(columnValues);
        Query.Row row = newRow(rawValues);

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.ROW)
                .setRowEvent(
                        Binlogdata.RowEvent.newBuilder()
                                .addRowChanges(Binlogdata.RowChange.newBuilder().setBefore(row).build())
                                .setTableName(TEST_VITESS_FULL_TABLE)
                                .setShard(TEST_SHARD)
                                .build())
                .setTimestamp(AnonymousValue.getLong())
                .build();
    }

    public static Binlogdata.VEvent defaultUpdateEvent() {
        return newUpdateEvent(defaultColumnValues(), defaultColumnValues());
    }

    public static Binlogdata.VEvent newUpdateEvent(
                                                   List<ColumnValue> beforeColumnValues, List<ColumnValue> afterColumnValues) {
        List<byte[]> beforeRawValues = newRawValues(beforeColumnValues);
        Query.Row beforeRow = newRow(beforeRawValues);
        List<byte[]> afterRawValues = newRawValues(afterColumnValues);
        Query.Row afterRow = newRow(afterRawValues);

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.ROW)
                .setRowEvent(
                        Binlogdata.RowEvent.newBuilder()
                                .addRowChanges(Binlogdata.RowChange.newBuilder().setBefore(beforeRow).setAfter(afterRow).build())
                                .setTableName(TEST_VITESS_FULL_TABLE)
                                .setShard(TEST_SHARD)
                                .build())
                .setTimestamp(AnonymousValue.getLong())
                .build();
    }

    public static List<ColumnValue> defaultColumnValues() {
        return Arrays.asList(
                new ColumnValue("bool_col", Query.Type.INT8, Types.SMALLINT, "1".getBytes(), (short) 1),
                new ColumnValue("int_col", Query.Type.INT32, Types.INTEGER, null, null),
                new ColumnValue("long_col", Query.Type.INT32, Types.BIGINT, "23".getBytes(), 23L),
                new ColumnValue("string_col", Query.Type.VARBINARY, Types.VARCHAR, "test".getBytes(), "test"));
    }

    public static List<ColumnValue> columnValuesSubset() {
        return Arrays.asList(
                new ColumnValue("bool_col", Query.Type.INT8, Types.SMALLINT, "1".getBytes(), (short) 1),
                new ColumnValue("int_col", Query.Type.INT32, Types.INTEGER, null, null),
                new ColumnValue("string_col", Query.Type.VARBINARY, Types.VARCHAR, "test".getBytes(), "test"));
    }

    public static List<byte[]> defaultRawValues() {
        return newRawValues(defaultColumnValues());
    }

    public static TableId defaultTableId() {
        return new TableId(TestHelper.TEST_SHARD, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE);
    }

    public static List<byte[]> newRawValues(List<ColumnValue> columnValues) {
        return columnValues.stream().map(x -> x.getRawValue()).collect(Collectors.toList());
    }

    public static int defaultNumOfColumns() {
        return defaultColumnValues().size();
    }

    public static int columnSubsetNumOfColumns() {
        return columnValuesSubset().size();
    }

    public static Query.Row defaultRow() {
        return newRow(defaultRawValues());
    }

    public static Query.Row newRow(List<byte[]> rawValues) {
        return Query.Row.newBuilder()
                .setValues(
                        ByteString.copyFrom(Bytes.concat(rawValues.stream().filter(Objects::nonNull).toArray(byte[][]::new))))
                .addAllLengths(
                        rawValues.stream()
                                .map(x -> x != null ? (long) x.length : -1L)
                                .collect(Collectors.toList()))
                .build();
    }

    public static List<Field> defaultFields() {
        return newFields(defaultColumnValues());
    }

    public static List<Field> fieldsSubset() {
        return newFields(columnValuesSubset());
    }

    public static List<Field> newFields(List<ColumnValue> columnValues) {
        return columnValues.stream().map(x -> x.getField()).collect(Collectors.toList());
    }

    public static List<ReplicationMessage.Column> defaultRelationMessageColumns() {
        return defaultColumnValues().stream()
                .map(x -> x.getReplicationMessageColumn())
                .collect(Collectors.toList());
    }

    public static List<Object> defaultJavaValues() {
        return defaultColumnValues().stream().map(x -> x.getJavaValue()).collect(Collectors.toList());
    }

    public static class ColumnValue {
        private final Field field;
        private final ReplicationMessageColumn replicationMessageColumn;
        private final Object javaValue;

        public ColumnValue(
                           String columnName, Query.Type queryType, int jdbcId, byte[] rawValue, Object javaValue) {
            this(columnName, queryType, jdbcId, rawValue, javaValue, Collections.emptyList(), "");
        }

        public ColumnValue(
                           String columnName, Query.Type queryType, int jdbcId, byte[] rawValue, Object javaValue, List<String> enumSetValues, String columnType) {
            this.field = Field.newBuilder().setName(columnName).setType(queryType).setColumnType(columnType).build();
            this.replicationMessageColumn = new ReplicationMessageColumn(
                    columnName, new VitessType(queryType.name(), jdbcId, enumSetValues), true, rawValue);
            this.javaValue = javaValue;
        }

        public Field getField() {
            return field;
        }

        public ReplicationMessageColumn getReplicationMessageColumn() {
            return replicationMessageColumn;
        }

        public byte[] getRawValue() {
            return replicationMessageColumn.getRawValue();
        }

        public Object getJavaValue() {
            return javaValue;
        }
    }
}
