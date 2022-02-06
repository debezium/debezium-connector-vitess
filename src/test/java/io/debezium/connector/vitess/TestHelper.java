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
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageColumn;
import io.debezium.connector.vitess.connection.VitessTabletType;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;

import binlogdata.Binlogdata;

public class TestHelper {
    protected static final String TEST_SERVER = "test_server";
    public static final String TEST_UNSHARDED_KEYSPACE = "test_unsharded_keyspace";
    public static final String TEST_SHARDED_KEYSPACE = "test_sharded_keyspace";
    public static final String TEST_SHARD = "0";
    public static final String TEST_TABLE = "test_table";
    private static final String TEST_VITESS_FULL_TABLE = TEST_UNSHARDED_KEYSPACE + "." + TEST_TABLE;
    protected static final String PK_FIELD = "id";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final String VTCTLD_HOST = "localhost";
    private static final int VTCTLD_PORT = 15999;
    private static final String VTGATE_HOST = "localhost";
    private static final int VTGATE_PORT = 15991;
    // Use the same username and password for vtgate and vtctld
    private static final String USERNAME = "vitess";
    private static final String PASSWORD = "vitess_password";

    protected static final String INSERT_STMT = "INSERT INTO t1 (int_col) VALUES (1);";
    protected static final List<String> SETUP_TABLES_STMT = Arrays.asList(
            "DROP TABLE IF EXISTS t1;",
            "CREATE TABLE t1 (id BIGINT NOT NULL AUTO_INCREMENT, int_col INT, PRIMARY KEY (id));");

    public static Configuration.Builder defaultConfig() {
        return defaultConfig(false);
    }

    /**
     * Get the default configuration of the connector
     *
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @return Configuration builder
     */
    public static Configuration.Builder defaultConfig(boolean hasMultipleShards) {
        Configuration.Builder builder = Configuration.create();
        builder = builder
                .with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(VitessConnectorConfig.VTGATE_HOST, VTGATE_HOST)
                .with(VitessConnectorConfig.VTGATE_PORT, VTGATE_PORT)
                .with(VitessConnectorConfig.VTGATE_USER, USERNAME)
                .with(VitessConnectorConfig.VTGATE_PASSWORD, PASSWORD)
                .with(VitessConnectorConfig.POLL_INTERVAL_MS, 100);
        if (hasMultipleShards) {
            return builder.with(VitessConnectorConfig.KEYSPACE, TEST_SHARDED_KEYSPACE);
        }
        else {
            return builder.with(VitessConnectorConfig.KEYSPACE, TEST_UNSHARDED_KEYSPACE)
                    .with(VitessConnectorConfig.SHARD, TEST_SHARD);
        }
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
            return vtctldConnection.applySchema(ddl, "online", keyspace);
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

    public static Binlogdata.VEvent newFieldEvent(List<ColumnValue> columnValues) {
        Binlogdata.FieldEvent.Builder fieldEventBuilder = Binlogdata.FieldEvent.newBuilder().setTableName(TEST_VITESS_FULL_TABLE);
        for (Field field : newFields(columnValues)) {
            fieldEventBuilder.addFields(field);
        }

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.FIELD)
                .setFieldEvent(fieldEventBuilder.build())
                .setTimestamp(AnonymousValue.getLong())
                .build();
    }

    public static Binlogdata.VEvent defaultInsertEvent() {
        return newInsertEvent(defaultColumnValues());
    }

    public static Binlogdata.VEvent newInsertEvent(List<ColumnValue> columnValues) {
        List<byte[]> rawValues = newRawValues(columnValues);
        Query.Row row = newRow(rawValues);

        return Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.ROW)
                .setRowEvent(
                        Binlogdata.RowEvent.newBuilder()
                                .addRowChanges(Binlogdata.RowChange.newBuilder().setAfter(row).build())
                                .setTableName(TEST_VITESS_FULL_TABLE)
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

    public static List<byte[]> defaultRawValues() {
        return newRawValues(defaultColumnValues());
    }

    public static List<byte[]> newRawValues(List<ColumnValue> columnValues) {
        return columnValues.stream().map(x -> x.getRawValue()).collect(Collectors.toList());
    }

    public static int defaultNumOfColumns() {
        return defaultColumnValues().size();
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
            this.field = Field.newBuilder().setName(columnName).setType(queryType).build();
            this.replicationMessageColumn = new ReplicationMessageColumn(
                    columnName, new VitessType(queryType.name(), jdbcId), true, rawValue);
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
