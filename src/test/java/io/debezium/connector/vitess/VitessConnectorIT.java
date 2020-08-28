/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_KEYSPACE;
import static junit.framework.TestCase.assertEquals;
import static org.fest.assertions.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;
import io.debezium.util.Testing;

public class VitessConnectorIT extends AbstractVitessConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorIT.class);

    private TestConsumer consumer;
    private VitessConnector connector;

    @Before
    public void before() {
        Testing.Print.enable();
    }

    @After
    public void after() {
        stopConnector();
    }

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new VitessConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();

        // also check that ALL_FIELDS is correctly configured
        VitessConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        // use an empty configuration which should be invalid because of the lack of DB connection
        // details
        Configuration config = Configuration.create().build();

        // we expect the engine will log at least one error, so preface it
        LOGGER.info(
                "Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exceptions will appear in the log");
        start(
                VitessConnector.class,
                config,
                (success, msg, error) -> {
                    assertThat(success).isFalse();
                    assertThat(error).isNotNull();
                });
        assertConnectorNotRunning();
    }

    @Test
    public void shouldValidateMinimalConfiguration() {
        Configuration config = TestHelper.defaultConfig().build();
        Config validateConfig = new VitessConnector().validate(config.asMap());
        validateConfig
                .configValues()
                .forEach(
                        configValue -> assertTrue(
                                "Unexpected error for: " + configValue.name(),
                                configValue.errorMessages().isEmpty()));
    }

    @Test
    public void shouldReceiveChangesForInsertsWithDifferentDataTypes() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        SourceRecord sourceRecord = assertInsert(
                INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), null);

        consumer.expects(expectedRecordsCount);
        sourceRecord = assertInsert(
                INSERT_STRING_TYPES_STMT,
                schemasAndValuesForStringTypes(),
                RecordOffset.fromSourceInfo(sourceRecord, expectedRecordsCount));

        consumer.expects(expectedRecordsCount);
        sourceRecord = assertInsert(
                INSERT_ENUM_TYPE_STMT,
                schemasAndValuesForEnumType(),
                RecordOffset.fromSourceInfo(sourceRecord, expectedRecordsCount));

        consumer.expects(expectedRecordsCount);
        sourceRecord = assertInsert(
                INSERT_SET_TYPE_STMT,
                schemasAndValuesForSetType(),
                RecordOffset.fromSourceInfo(sourceRecord, expectedRecordsCount));

        consumer.expects(expectedRecordsCount);
        assertInsert(
                INSERT_TIME_TYPES_STMT,
                schemasAndValuesForTimeType(),
                RecordOffset.fromSourceInfo(sourceRecord, expectedRecordsCount));

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldOffsetContainMultipleEventSkips() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        // insert 1 row to get the initial vgtid
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        SourceRecord sourceRecord = assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), null);

        // insert 2 rows
        expectedRecordsCount = 2;
        List<String> two_inserts = new ArrayList<>(expectedRecordsCount);
        IntStream.rangeClosed(1, expectedRecordsCount).forEach(i -> two_inserts.add(INSERT_NUMERIC_TYPES_STMT));
        consumer.expects(expectedRecordsCount);
        TableId table = tableIdFromInsertStmt(INSERT_NUMERIC_TYPES_STMT);
        executeAndWait(two_inserts);

        for (int expectedEventsToSkip = 1; expectedEventsToSkip <= expectedRecordsCount; expectedEventsToSkip++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT));
            // verify the offset has correct eventsToSkip value
            assertRecordOffset(record, RecordOffset.fromSourceInfo(sourceRecord, expectedEventsToSkip));
            assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldOffsetIncrementAfterDDL() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        // insert 1 row to get the initial vgtid
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        SourceRecord sourceRecord = assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), null);

        // apply DDL
        TestHelper.execute("ALTER TABLE numeric_table ADD foo INT default 10;");

        // insert 1 row
        int numOfGtidsFromDdl = 2;
        consumer.expects(expectedRecordsCount);
        List<SchemaAndValueField> expectedSchemaAndValuesByColumn = schemasAndValuesForNumericTypes();
        expectedSchemaAndValuesByColumn.add(
                new SchemaAndValueField("foo", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        RecordOffset expectedOffset = RecordOffset
                .fromSourceInfo(sourceRecord, expectedRecordsCount)
                .incrementOffset(numOfGtidsFromDdl);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, expectedSchemaAndValuesByColumn, expectedOffset);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldReceiveMultipleRowsInSameStmt() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 2;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        String insertTwoRowsInSameStmt = "INSERT INTO numeric_table ("
                + "tinyint_col,"
                + "smallint_col,"
                + "mediumint_col,"
                + "int_col,"
                + "bigint_col,"
                + "float_col,"
                + "double_col,"
                + "decimal_col,"
                + "boolean_col)"
                + " VALUES (1, 12, 123, 1234, 12345, 1.5, 2.5, 12.34, true), (1, 12, 123, 1234, 12345, 1.5, 2.5, 12.34, true);";
        // wait to receive 2 rows, but only verify the first row is enough
        assertInsert(insertTwoRowsInSameStmt, schemasAndValuesForNumericTypes(), null);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldUsePrevVgtidAsOffsetWhenNoVgtidInGrpcResponse() throws Exception {
        Testing.Print.disable();
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        // Insert 1 row to get the starting offset
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        consumer.expects(expectedRecordsCount);
        // verify outcome: offset should be x
        final SourceRecord previousRecord = assertInsert(
                INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), null);

        // Insert 1000 rows
        // We should get multiple gRPC responses:
        // The first response contains BEGIN and ROW events; The last response contains ROW, VGTID and COMMIT events.
        expectedRecordsCount = 1000;
        consumer.expects(expectedRecordsCount);
        String rowValue = "(1, 12, 123, 1234, 12345, 1.5, 2.5, 12.34, true)";
        StringBuilder insertRows = new StringBuilder().append("INSERT INTO numeric_table ("
                + "tinyint_col,"
                + "smallint_col,"
                + "mediumint_col,"
                + "int_col,"
                + "bigint_col,"
                + "float_col,"
                + "double_col,"
                + "decimal_col,"
                + "boolean_col)"
                + " VALUES " + rowValue);
        for (int i = 1; i < expectedRecordsCount; i++) {
            insertRows.append(", ").append(rowValue);
        }

        String insertRowsStatement = insertRows.toString();
        try {
            // exercise SUT
            executeAndWait(insertRowsStatement);
            for (int eventsToSkip = 1; eventsToSkip <= expectedRecordsCount; eventsToSkip++) {
                SourceRecord actualRecord = assertRecordInserted(TEST_KEYSPACE + ".numeric_table");
                // verify outcome: offset should be x + 1
                assertRecordOffset(actualRecord, RecordOffset.fromSourceInfo(previousRecord, eventsToSkip));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // Insert 1 row
        expectedRecordsCount = 1;
        consumer.expects(expectedRecordsCount);
        // verify outcome: offset should be x + 2
        assertInsert(
                INSERT_NUMERIC_TYPES_STMT,
                schemasAndValuesForNumericTypes(),
                RecordOffset.fromSourceInfo(previousRecord, expectedRecordsCount).incrementOffset(1));
        stopConnector();
        assertConnectorNotRunning();
        Testing.Print.enable();
    }

    private void startConnector() throws InterruptedException {
        startConnector(Function.identity());
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig)
            throws InterruptedException {
        Configuration.Builder configBuilder = customConfig.apply(TestHelper.defaultConfig());
        start(VitessConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        waitForStreamingRunning();
    }

    private void waitForStreamingRunning() throws InterruptedException {
        waitForStreamingRunning(Module.name(), TestHelper.TEST_SERVER);
    }

    private SourceRecord assertInsert(
                                      String statement,
                                      List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                      RecordOffset expectedRecordOffset) {
        TableId table = tableIdFromInsertStmt(statement);

        try {
            executeAndWait(statement);
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(statement));
            assertRecordOffset(record, expectedRecordOffset);
            assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());
            assertRecordSchemaAndValues(
                    expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
            return record;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SourceRecord assertRecordInserted(String expectedTopicName) {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();
        return assertRecordInserted(insertedRecord, expectedTopicName);
    }

    private SourceRecord assertRecordUpdated() {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord updatedRecord = consumer.remove();
        return assertRecordUpdated(updatedRecord);
    }

    private SourceRecord assertRecordInserted(SourceRecord insertedRecord, String expectedTopicName) {
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());
        VerifyRecord.isValidInsert(insertedRecord);
        return insertedRecord;
    }

    private SourceRecord assertRecordUpdated(SourceRecord updatedRecord) {
        VerifyRecord.isValidUpdate(updatedRecord);
        return updatedRecord;
    }

    private void executeAndWait(String statement) throws Exception {
        executeAndWait(Collections.singletonList(statement));
    }

    private void executeAndWait(List<String> statements) throws Exception {
        TestHelper.execute(statements);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
    }

    private static String topicName(String suffix) {
        return TestHelper.TEST_SERVER + "." + suffix;
    }

    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
        assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
        assertThat(key.dependents).isEqualTo(expected.dependents());
        assertThat(key.width).isNotNull();
        assertThat(key.group).isNotNull();
        assertThat(key.orderInGroup).isGreaterThan(0);
        assertThat(key.validator).isNull();
        assertThat(key.recommender).isNull();
    }
}
