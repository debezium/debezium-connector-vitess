/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
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
    @FixFor("DBZ-2776")
    public void shouldReceiveChangesForInsertsWithDifferentDataTypes() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_STRING_TYPES_STMT, schemasAndValuesForStringTypes(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_ENUM_TYPE_STMT, schemasAndValuesForEnumType(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_SET_TYPE_STMT, schemasAndValuesForSetType(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_TIME_TYPES_STMT, schemasAndValuesForTimeType(), TestHelper.PK_FIELD);

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
        SourceRecord sourceRecord = assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);

        // apply DDL
        TestHelper.execute("ALTER TABLE numeric_table ADD foo INT default 10;");
        // applying DDL for Vitess version v8.0.0 emits 1 gRPC responses: (VGTID, DDL)
        // the VGTID is increased by 1.
        int numOfGtidsFromDdl = 1;

        // insert 1 row
        consumer.expects(expectedRecordsCount);
        List<SchemaAndValueField> expectedSchemaAndValuesByColumn = schemasAndValuesForNumericTypes();
        expectedSchemaAndValuesByColumn.add(
                new SchemaAndValueField("foo", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        SourceRecord sourceRecord2 = assertInsert(INSERT_NUMERIC_TYPES_STMT, expectedSchemaAndValuesByColumn, TestHelper.PK_FIELD);

        String expectedOffset = RecordOffset
                .fromSourceInfo(sourceRecord)
                .incrementOffset(numOfGtidsFromDdl + 1).getVgtid();
        String actualOffset = (String) sourceRecord2.sourceOffset().get(SourceInfo.VGTID);
        Assert.assertEquals(expectedOffset, actualOffset);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldSameTransactionLastRowOffsetBeNewVgtid() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        // insert 1 row to get the initial vgtid
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        SourceRecord sourceRecord = assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);

        // insert 2 rows
        expectedRecordsCount = 2;
        List<String> two_inserts = new ArrayList<>(expectedRecordsCount);
        IntStream.rangeClosed(1, expectedRecordsCount).forEach(i -> two_inserts.add(INSERT_NUMERIC_TYPES_STMT));
        consumer.expects(expectedRecordsCount);
        TableId table = tableIdFromInsertStmt(INSERT_NUMERIC_TYPES_STMT);
        executeAndWait(two_inserts);

        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
            if (i != expectedRecordsCount) {
                // other row events have the previous vgtid
                assertRecordOffset(record, RecordOffset.fromSourceInfo(sourceRecord));
            }
            else {
                // last row event has the new vgtid
                assertRecordOffset(record, RecordOffset.fromSourceInfo(record));
            }
            assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldMultipleRowsInSameStmtLastRowOffsetBeNewVgtid() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 2;
        consumer = testConsumer(expectedRecordsCount);

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        // insert 2 rows
        String insertTwoRowsInSameStmt = "INSERT INTO numeric_table ("
                + "tinyint_col,"
                + "tinyint_unsigned_col,"
                + "smallint_col,"
                + "smallint_unsigned_col,"
                + "mediumint_col,"
                + "mediumint_unsigned_col,"
                + "int_col,"
                + "int_unsigned_col,"
                + "bigint_col,"
                + "bigint_unsigned_col,"
                + "bigint_unsigned_overflow_col,"
                + "float_col,"
                + "double_col,"
                + "decimal_col,"
                + "boolean_col)"
                + " VALUES (1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true), (1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true);";
        executeAndWait(insertTwoRowsInSameStmt);
        TableId table = tableIdFromInsertStmt(insertTwoRowsInSameStmt);

        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(insertTwoRowsInSameStmt), TestHelper.PK_FIELD);
            if (i != expectedRecordsCount) {
                // other row events have the previous vgtid
                assertRecordOffset(record, new RecordOffset(baseVgtid.toString()));
            }
            else {
                // last row event has the new vgtid
                assertRecordOffset(record, RecordOffset.fromSourceInfo(record));
            }
            assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void shouldUsePrevVgtidAsOffsetWhenNoVgtidInGrpcResponse() throws Exception {
        Testing.Print.disable();
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        // Insert 1000 rows
        // We should get multiple gRPC responses:
        // The first response contains BEGIN and ROW events; The last response contains ROW, VGTID and COMMIT events.
        int expectedRecordsCount = 1000;
        consumer = testConsumer(expectedRecordsCount);
        String rowValue = "(1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true)";
        StringBuilder insertRows = new StringBuilder().append("INSERT INTO numeric_table ("
                + "tinyint_col,"
                + "tinyint_unsigned_col,"
                + "smallint_col,"
                + "smallint_unsigned_col,"
                + "mediumint_col,"
                + "mediumint_unsigned_col,"
                + "int_col,"
                + "int_unsigned_col,"
                + "bigint_col,"
                + "bigint_unsigned_col,"
                + "bigint_unsigned_overflow_col,"
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
            for (int i = 1; i <= expectedRecordsCount; i++) {
                SourceRecord actualRecord = assertRecordInserted(TestHelper.TEST_UNSHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                if (i != expectedRecordsCount) {
                    // other row events have the previous vgtid
                    assertRecordOffset(actualRecord, new RecordOffset(baseVgtid.toString()));
                }
                else {
                    // last row event has the new vgtid
                    assertRecordOffset(actualRecord, RecordOffset.fromSourceInfo(actualRecord));
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        stopConnector();
        assertConnectorNotRunning();
        Testing.Print.enable();
    }

    @Test
    public void shouldMultiShardSubscriptionHaveMultiShardGtidsInVgtid() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TestHelper.TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldUseMultiColumnPkAsRecordKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TestHelper.TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        final String insertStatement = "INSERT INTO comp_pk_table (int_col, int_col2) VALUES (1, 2);";
        executeAndWait(insertStatement, TestHelper.TEST_SHARDED_KEYSPACE);
        final SourceRecord record = consumer.remove();
        final String expectedTopicName = topicNameFromInsertStmt(insertStatement, TestHelper.TEST_SHARDED_KEYSPACE);
        TableId table = tableIdFromInsertStmt(insertStatement, TestHelper.TEST_SHARDED_KEYSPACE);

        // Record key has all columns from the multi-column primary key
        assertRecordInserted(record, expectedTopicName, TestHelper.PK_FIELD);
        assertRecordInserted(record, expectedTopicName, "int_col");
        assertRecordOffset(record, hasMultipleShards);
        assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldUseUniqueKeyAsRecordKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TestHelper.TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        // Record key is the unique key if no primary key
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_multi_unique_keys_table (int_col, int_col2) VALUES (1, 2);", null, TestHelper.TEST_SHARDED_KEYSPACE, "int_col",
                hasMultipleShards);

        // Record key is the unique key, not the multi-column composite key
        consumer.expects(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_multi_comp_unique_keys_table (int_col, int_col2, int_col3, int_col4, int_col5) VALUES (1, 2, 3, 4, 5);", null,
                TestHelper.TEST_SHARDED_KEYSPACE, "int_col3", hasMultipleShards);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldNotHaveRecordKeyIfNoPrimaryKeyUniqueKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TestHelper.TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_table (int_col) VALUES (1);", null, TestHelper.TEST_SHARDED_KEYSPACE, null, hasMultipleShards);

        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldPrioritizePrimaryKeyAsRecordKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TestHelper.TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO pk_single_unique_key_table (int_col) VALUES (1);", null, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);

        stopConnector();
        assertConnectorNotRunning();
    }

    private void startConnector() throws InterruptedException {
        startConnector(false);
    }

    /**
     * Start the connector.
     *
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @throws InterruptedException
     */
    private void startConnector(boolean hasMultipleShards) throws InterruptedException {
        startConnector(Function.identity(), hasMultipleShards);
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig, boolean hasMultipleShards)
            throws InterruptedException {
        Configuration.Builder configBuilder = customConfig.apply(TestHelper.defaultConfig(hasMultipleShards));
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
                                      String pkField) {
        return assertInsert(statement, expectedSchemaAndValuesByColumn, TestHelper.TEST_UNSHARDED_KEYSPACE, pkField, false);
    }

    /**
     * Assert that the connector receives a valid insert event.
     *
     * @param statement The insert sql statement
     * @param expectedSchemaAndValuesByColumn The expected column type and value
     * @param database The database (a.k.a keyspace or schema) name
     * @param pkField The primary key column's name
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @return The {@link SourceRecord} generated from the insert event
     */
    private SourceRecord assertInsert(
                                      String statement,
                                      List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                      String database,
                                      String pkField,
                                      boolean hasMultipleShards) {
        TableId table = tableIdFromInsertStmt(statement, database);

        try {
            executeAndWait(statement, database);
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(statement, database), pkField);
            assertRecordOffset(record, hasMultipleShards);
            assertSourceInfo(record, TestHelper.TEST_SERVER, table.schema(), table.table());
            if (expectedSchemaAndValuesByColumn != null && !expectedSchemaAndValuesByColumn.isEmpty()) {
                assertRecordSchemaAndValues(
                        expectedSchemaAndValuesByColumn, record, Envelope.FieldName.AFTER);
            }
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

    private SourceRecord assertRecordInserted(String expectedTopicName, String pkField) {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();
        return assertRecordInserted(insertedRecord, expectedTopicName, pkField);
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

    private SourceRecord assertRecordInserted(SourceRecord insertedRecord, String expectedTopicName, String pkField) {
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());
        if (pkField != null) {
            VitessVerifyRecord.isValidInsert(insertedRecord, pkField);
        }
        else {
            VerifyRecord.isValidInsert(insertedRecord);
        }
        return insertedRecord;
    }

    private SourceRecord assertRecordUpdated(SourceRecord updatedRecord) {
        VerifyRecord.isValidUpdate(updatedRecord);
        return updatedRecord;
    }

    private void executeAndWait(String statement) throws Exception {
        executeAndWait(statement, TestHelper.TEST_UNSHARDED_KEYSPACE);
    }

    private void executeAndWait(String statement, String database) throws Exception {
        executeAndWait(Collections.singletonList(statement), database);
    }

    private void executeAndWait(List<String> statements) throws Exception {
        executeAndWait(statements, TestHelper.TEST_UNSHARDED_KEYSPACE);
    }

    private void executeAndWait(List<String> statements, String database) throws Exception {
        TestHelper.execute(statements, database);
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
