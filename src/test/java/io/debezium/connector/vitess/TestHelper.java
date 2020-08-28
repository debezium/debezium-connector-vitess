/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageColumn;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;

import binlogdata.Binlogdata;

public class TestHelper {
    protected static final String TEST_SERVER = "test_server";
    public static final String TEST_KEYSPACE = "commerce";
    public static final String TEST_TABLE = "test_table";
    private static final String TEST_VITESS_FULL_TABLE = TEST_KEYSPACE + "." + TEST_TABLE;
    protected static final String PK_FIELD = "id";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";

    public static Configuration.Builder defaultConfig() {
        Configuration.Builder builder = Configuration.create();
        builder
                .with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(VitessConnectorConfig.VTGATE_HOST, "localhost")
                .with(VitessConnectorConfig.VTGATE_PORT, 15991)
                .with(VitessConnectorConfig.KEYSPACE, TEST_KEYSPACE)
                .with(VitessConnectorConfig.SHARD, 0)
                .with(VitessConnectorConfig.VTCTLD_HOST, "localhost")
                .with(VitessConnectorConfig.VTCTLD_PORT, 15999)
                .with(VitessConnectorConfig.POLL_INTERVAL_MS, 100);
        return builder;
    }

    /**
     * Executes a JDBC statement using the default jdbc config without autocommitting the connection
     * @param statements A list of SQL statements
     */
    public static void execute(List<String> statements) {

        try (MySQLConnection connection = MySQLConnection.forTestDatabase()) {
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
        execute(Collections.singletonList(statement));
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI())).stream()
                .filter(line -> !line.isEmpty())
                .collect(Collectors.joining(System.lineSeparator()));
        execute(Arrays.asList(statements.split(";")));
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
        List<String> rawValues = newRawValues(columnValues);
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
        List<String> rawValues = newRawValues(columnValues);
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
        List<String> beforeRawValues = newRawValues(beforeColumnValues);
        Query.Row beforeRow = newRow(beforeRawValues);
        List<String> afterRawValues = newRawValues(afterColumnValues);
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
                new ColumnValue("bool_col", Query.Type.INT8, Types.SMALLINT, "1", (short) 1),
                new ColumnValue("int_col", Query.Type.INT32, Types.INTEGER, null, null),
                new ColumnValue("long_col", Query.Type.INT32, Types.BIGINT, "23", 23L),
                new ColumnValue("string_col", Query.Type.VARBINARY, Types.VARCHAR, "test", "test"));
    }

    public static List<String> defaultRawValues() {
        return newRawValues(defaultColumnValues());
    }

    public static List<String> newRawValues(List<ColumnValue> columnValues) {
        return columnValues.stream().map(x -> x.getRawValue()).collect(Collectors.toList());
    }

    public static int defaultNumOfColumns() {
        return defaultColumnValues().size();
    }

    public static Query.Row defaultRow() {
        return newRow(defaultRawValues());
    }

    public static Query.Row newRow(List<String> rawValues) {
        return Query.Row.newBuilder()
                .setValues(
                        ByteString.copyFrom(
                                rawValues.stream().filter(Objects::nonNull).collect(Collectors.joining()),
                                StandardCharsets.UTF_8))
                .addAllLengths(
                        defaultRawValues().stream()
                                .map(x -> x != null ? (long) x.length() : -1L)
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
                           String columnName, Query.Type queryType, int jdbcId, String rawValue, Object javaValue) {
            this.field = Field.newBuilder().setName(columnName).setType(queryType).build();
            this.replicationMessageColumn = new ReplicationMessageColumn(
                    columnName, new VitessType(columnName, jdbcId), true, rawValue);
            this.javaValue = javaValue;
        }

        public Field getField() {
            return field;
        }

        public ReplicationMessageColumn getReplicationMessageColumn() {
            return replicationMessageColumn;
        }

        public String getRawValue() {
            return replicationMessageColumn.getRawValue();
        }

        public Object getJavaValue() {
            return javaValue;
        }
    }
}
