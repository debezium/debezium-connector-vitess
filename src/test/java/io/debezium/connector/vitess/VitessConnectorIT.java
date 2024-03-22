/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.TableId;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

public class VitessConnectorIT extends AbstractVitessConnectorTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorIT.class);

    private TestConsumer consumer;
    private VitessConnector connector;
    private AtomicBoolean isConnectorRunning = new AtomicBoolean(false);

    @Before
    public void before() {
        Testing.Print.enable();
    }

    @After
    public void after() {
        stopConnector();
        assertConnectorNotRunning();
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
    }

    @Test
    public void shouldValidateMinimalConfiguration() {
        Configuration config = TestHelper.defaultConfig().build();
        VitessConnector connector = new VitessConnector();
        connector.start(config.asMap());
        Config validateConfig = connector.validate(config.asMap());
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
        assertInsert(INSERT_BYTES_TYPES_STMT, schemasAndValuesForBytesTypesAsBytes(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_ENUM_TYPE_STMT, schemasAndValuesForEnumType(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_SET_TYPE_STMT, schemasAndValuesForSetType(), TestHelper.PK_FIELD);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_TIME_TYPES_STMT, schemasAndValuesForTimeType(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldConsumeEventsWithTruncatedColumn() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(builder -> builder.with("column.truncate.to.1.chars",
                TEST_UNSHARDED_KEYSPACE + ".string_table.mediumtext_col"), false,
                false, 1, -1, -1, null,
                VitessConnectorConfig.SnapshotMode.NEVER, "");
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_STRING_TYPES_STMT, schemasAndValuesForStringTypesTruncated(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveBytesAsBytes() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(CommonConnectorConfig.BINARY_HANDLING_MODE, CommonConnectorConfig.BinaryHandlingMode.BYTES), false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_BYTES_TYPES_STMT, schemasAndValuesForBytesTypesAsBytes(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveBytesAsBase64String() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(VitessConnectorConfig.BINARY_HANDLING_MODE, VitessConnectorConfig.BinaryHandlingMode.BASE64), false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_BYTES_TYPES_STMT, schemasAndValuesForBytesTypesAsBase64String(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveBytesAsHexString() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(VitessConnectorConfig.BINARY_HANDLING_MODE, VitessConnectorConfig.BinaryHandlingMode.HEX), false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_BYTES_TYPES_STMT, schemasAndValuesForBytesTypesAsHexString(), TestHelper.PK_FIELD);
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
        int numOfGtidsFromDdl = 1;

        // As of Vitess 17.0.3 four additional VGTID events are emitted after the DDL and before the next insert
        int numOfAdditionalGtids = 4;

        // insert 1 row
        consumer.expects(expectedRecordsCount);
        List<SchemaAndValueField> expectedSchemaAndValuesByColumn = schemasAndValuesForNumericTypes();
        expectedSchemaAndValuesByColumn.add(
                new SchemaAndValueField("foo", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        SourceRecord sourceRecord2 = assertInsert(INSERT_NUMERIC_TYPES_STMT, expectedSchemaAndValuesByColumn, TestHelper.PK_FIELD);

        String expectedOffset = RecordOffset
                .fromSourceInfo(sourceRecord)
                .incrementOffset(numOfGtidsFromDdl + numOfAdditionalGtids + 1).getVgtid();
        String actualOffset = (String) sourceRecord2.sourceOffset().get(SourceInfo.VGTID_KEY);
        Assert.assertEquals(expectedOffset, actualOffset);
    }

    @Test
    @FixFor("DBZ-4353")
    public void shouldSchemaUpdatedAfterOnlineDdl() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
        // Add a column using online ddl and wait until it is finished
        String ddlId = TestHelper.applyOnlineDdl("ALTER TABLE numeric_table ADD COLUMN foo INT", TEST_UNSHARDED_KEYSPACE);
        Awaitility
                .await()
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> TestHelper.checkOnlineDDL(TEST_UNSHARDED_KEYSPACE, ddlId));
        // Do another insert with the new column and verify it is in the SourceRecord
        List<SchemaAndValueField> expectedSchemaAndValuesByColumn = schemasAndValuesForNumericTypes();
        String dml = "INSERT INTO numeric_table ("
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
                + "boolean_col,"
                + "foo)"
                + " VALUES (1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true, 10);";
        expectedSchemaAndValuesByColumn.add(
                new SchemaAndValueField("foo", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        assertInsert(dml, expectedSchemaAndValuesByColumn, TestHelper.PK_FIELD);
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
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, table.table());
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }
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
                String newVgtid = RecordOffset.fromSourceInfo(record).getVgtid();
                assertThat(newVgtid).isNotNull();
                assertThat(newVgtid).isNotEqualTo(baseVgtid.toString());
            }
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, table.table());
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }
    }

    @Test
    public void shouldUsePrevVgtidAsOffsetWhenNoVgtidInGrpcResponse() throws Exception {
        Testing.Print.disable();
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        // Insert 10000 rows to make sure we will get multiple gRPC responses.
        // We should get multiple gRPC responses:
        // The first response contains BEGIN and ROW events; The last response contains ROW, VGTID and COMMIT events.
        int expectedRecordsCount = 10000;
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
                SourceRecord actualRecord = assertRecordInserted(TEST_UNSHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                if (i != expectedRecordsCount) {
                    // other row events have the previous vgtid
                    assertRecordOffset(actualRecord, new RecordOffset(baseVgtid.toString()));
                }
                else {
                    // last row event has the new vgtid
                    String newVgtid = RecordOffset.fromSourceInfo(actualRecord).getVgtid();
                    assertThat(newVgtid).isNotNull();
                    assertThat(newVgtid).isNotEqualTo(baseVgtid.toString());
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        Testing.Print.enable();
    }

    @Test
    @FixFor("")
    public void shouldTransactionMetadataUseLocalShard() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(VitessConnectorConfig.PROVIDE_ORDERED_TRANSACTION_METADATA, true)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(VitessConnectorConfig.SHARD, "-80,80-"),
                true,
                "80-");
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount + 2);

        String rowValue = "(1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true)";
        String insertQuery = "INSERT INTO numeric_table ("
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
                + " VALUES " + rowValue;
        StringBuilder insertRows = new StringBuilder().append(insertQuery);
        for (int i = 1; i < expectedRecordsCount; i++) {
            insertRows.append(", ").append(rowValue);
        }

        String insertRowsStatement = insertRows.toString();
        try {
            // exercise SUT
            executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
            // First transaction.
            SourceRecord beginRecord = assertRecordBeginSourceRecord();
            assertThat(beginRecord.sourceOffset()).containsKey("transaction_epoch");
            String expectedTxId1 = ((Struct) beginRecord.value()).getString("id");
            for (int i = 1; i <= expectedRecordsCount; i++) {
                SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                final Struct txn = ((Struct) record.value()).getStruct("transaction");
                String txId = txn.getString("id");
                assertThat(txId).isNotNull();
                assertThat(txId).isEqualTo(expectedTxId1);
                Vgtid actualVgtid = Vgtid.of(txId);
                // The current vgtid is not the previous vgtid.
                assertThat(actualVgtid).isNotEqualTo(baseVgtid);
            }
            assertRecordEnd(expectedTxId1, expectedRecordsCount);
        }
        catch (Exception e) {
        }
    }

    @Test
    @FixFor("")
    public void shouldTransactionMetadataUseLocalShard2() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(VitessConnectorConfig.SHARD, "-80,80-"),
                true,
                "80-");
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount + 2);

        String rowValue = "(1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true)";
        String insertQuery = "INSERT INTO numeric_table ("
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
                + " VALUES " + rowValue;
        StringBuilder insertRows = new StringBuilder().append(insertQuery);
        for (int i = 1; i < expectedRecordsCount; i++) {
            insertRows.append(", ").append(rowValue);
        }

        String insertRowsStatement = insertRows.toString();
        try {
            // exercise SUT
            executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
            // First transaction.
            String expectedTxId1 = assertRecordBegin();
            for (int i = 1; i <= expectedRecordsCount; i++) {
                SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                final Struct txn = ((Struct) record.value()).getStruct("transaction");
                String txId = txn.getString("id");
                assertThat(txId).isNotNull();
                assertThat(txId).isEqualTo(expectedTxId1);
                Vgtid actualVgtid = Vgtid.of(txId);
                // The current vgtid is not the previous vgtid.
                assertThat(actualVgtid).isNotEqualTo(baseVgtid);
            }
            assertRecordEnd(expectedTxId1, expectedRecordsCount);
        }
        catch (Exception e) {
        }
    }

    @Test
    @FixFor("DBZ-5063")
    public void shouldUseSameTransactionIdWhenMultiGrpcResponses() throws Exception {
        Testing.Print.disable();
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true), false);
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        // Insert 10000 rows to make sure we will get multiple gRPC responses.
        // The first response contains BEGIN and ROW events; The last response contains ROW, VGTID and COMMIT events.
        int expectedRecordsCount1 = 10000;
        // Insert 2 rows which can fit in a single gRPC response.
        int expectedRecordsCount2 = 2;
        // Expect expectedRecordsCount1 + expectedRecordsCount2 + 4 transaction metadata.
        consumer = testConsumer(expectedRecordsCount1 + expectedRecordsCount2 + 4);

        String rowValue = "(1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true)";
        String insertQuery = "INSERT INTO numeric_table ("
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
                + " VALUES " + rowValue;
        // Prepare first transaction.
        StringBuilder insertRows1 = new StringBuilder().append(insertQuery);
        for (int i = 1; i < expectedRecordsCount1; i++) {
            insertRows1.append(", ").append(rowValue);
        }
        // Prepare second transaction.
        StringBuilder insertRows2 = new StringBuilder().append(insertQuery);
        for (int i = 1; i < expectedRecordsCount2; i++) {
            insertRows2.append(", ").append(rowValue);
        }

        String insertRowsStatement1 = insertRows1.toString();
        String insertRowsStatement2 = insertRows2.toString();
        try {
            // exercise SUT
            TestHelper.execute(insertRowsStatement1);
            executeAndWait(insertRowsStatement2);
            // First transaction.
            String expectedTxId1 = assertRecordBegin();
            for (int i = 1; i <= expectedRecordsCount1; i++) {
                SourceRecord record = assertRecordInserted(TEST_UNSHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                final Struct txn = ((Struct) record.value()).getStruct("transaction");
                final Struct source = ((Struct) record.value()).getStruct("source");
                String txId = txn.getString("id");
                assertThat(txId).isNotNull();
                assertThat(txId).isEqualTo(expectedTxId1);
                // The current vgtid is not the previous vgtid.
                assertThat(txId).isNotEqualTo(baseVgtid.getShardGtid(source.getString("shard")).getGtid());
            }
            assertRecordEnd(expectedTxId1, expectedRecordsCount1);

            // Second transaction.
            String expectedTxId2 = assertRecordBegin();
            for (int i = 1; i <= expectedRecordsCount2; i++) {
                SourceRecord record = assertRecordInserted(TEST_UNSHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
                final Struct txn = ((Struct) record.value()).getStruct("transaction");
                String txId = txn.getString("id");
                assertThat(txId).isNotNull();
                assertThat(txId).isEqualTo(expectedTxId2);
                // The current vgtid is not the previous vgtid.
                assertThat(txId).isNotEqualTo(expectedTxId1);
            }
            assertRecordEnd(expectedTxId2, expectedRecordsCount2);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        Testing.Print.enable();
    }

    @Test
    public void shouldMultiShardSubscriptionHaveMultiShardGtidsInVgtid() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);
    }

    @Test
    public void shouldMultiShardConfigSubscriptionHaveMultiShardGtidsInVgtid() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(true, "-80,80-");
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);
    }

    @Test
    public void shouldMultiShardMultiTaskConfigSubscriptionHaveMultiShardGtidsInVgtid() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(Function.identity(), hasMultipleShards, true, 2, 0, 1, null, null, null);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldUseMultiColumnPkAsRecordKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        final String insertStatement = "INSERT INTO comp_pk_table (int_col, int_col2) VALUES (1, 2);";
        executeAndWait(insertStatement, TEST_SHARDED_KEYSPACE);
        final SourceRecord record = consumer.remove();
        final String expectedTopicName = topicNameFromInsertStmt(insertStatement, TEST_SHARDED_KEYSPACE);
        TableId table = tableIdFromInsertStmt(insertStatement, TEST_SHARDED_KEYSPACE);

        // Record key has all columns from the multi-column primary key
        assertRecordInserted(record, expectedTopicName, TestHelper.PK_FIELD);
        assertRecordInserted(record, expectedTopicName, "int_col");
        assertRecordOffset(record, hasMultipleShards);
        assertSourceInfo(record, TEST_SERVER, TEST_SHARDED_KEYSPACE, table.table());
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldUseUniqueKeyAsRecordKey() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        waitForShardedGtidAcquiring(logInterceptor);

        // Record key is the unique key if no primary key
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_multi_unique_keys_table (int_col, int_col2) VALUES (1, 2);", null, TEST_SHARDED_KEYSPACE, "int_col",
                hasMultipleShards);

        // Record key is the unique key, not the multi-column composite key
        consumer.expects(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_multi_comp_unique_keys_table (int_col, int_col2, int_col3, int_col4, int_col5) VALUES (1, 2, 3, 4, 5);", null,
                TEST_SHARDED_KEYSPACE, "int_col3", hasMultipleShards);
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldNotHaveRecordKeyIfNoPrimaryKeyUniqueKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO no_pk_table (int_col) VALUES (1);", null, TEST_SHARDED_KEYSPACE, null, hasMultipleShards);
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldConvertVarcharCharacterSetCollateColumnToString() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        String varcharCharacterSetCollateColumnAsciiBin = "varchar_character_set_ascii_collate_ascii_bin_col";
        String varcharCharacterSetCollateColumnAscii = "varchar_character_set_ascii_collate_ascii_col";
        String varcharCharacterSetCollateColumnLatin1Bin = "varchar_character_set_ascii_collate_latin1_bin_col";
        String varcharColumn = "varchar_col";
        String varbinaryColumn = "varbinary_col";
        String expectedVarchar = "foo";
        String query = String.format("INSERT INTO character_set_collate_table (%s, %s, %s, %s, %s) VALUES (\"%s\", \"%s\", \"%s\", \"%s\", \"%s\");",
                varcharCharacterSetCollateColumnAsciiBin,
                varcharCharacterSetCollateColumnAscii,
                varcharCharacterSetCollateColumnLatin1Bin,
                varcharColumn,
                varbinaryColumn,
                expectedVarchar,
                expectedVarchar,
                expectedVarchar,
                expectedVarchar,
                expectedVarchar);
        SourceRecord record = assertInsert(query, null, TEST_SHARDED_KEYSPACE, null, hasMultipleShards);
        Struct recordValueStruct = (Struct) record.value();
        Struct afterStruct = (Struct) recordValueStruct.get("after");
        Object actualVarchar = afterStruct.get(varcharColumn);
        assertThat(actualVarchar).isEqualTo(expectedVarchar);
        assertThat(afterStruct.get(varcharCharacterSetCollateColumnAsciiBin)).isEqualTo(actualVarchar);
        assertThat(afterStruct.get(varcharCharacterSetCollateColumnAscii)).isEqualTo(actualVarchar);
        assertThat(afterStruct.get(varcharCharacterSetCollateColumnLatin1Bin)).isEqualTo(actualVarchar);
        assertThat(afterStruct.get(varbinaryColumn).getClass()).isNotEqualTo(actualVarchar);
    }

    @Test
    @FixFor("DBZ-2578")
    public void shouldPrioritizePrimaryKeyAsRecordKey() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(hasMultipleShards);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert("INSERT INTO pk_single_unique_key_table (int_col) VALUES (1);", null, TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);
    }

    @Test
    @FixFor("DBZ-2836")
    public void shouldTaskFailIfColumnNameInvalid() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(VitessErrorHandler.class);
        TestHelper.executeDDL("vitess_create_tables.ddl");

        EmbeddedEngine.CompletionCallback completionCallback = (success, message, error) -> {
            isConnectorRunning.set(false);
        };
        start(VitessConnector.class, TestHelper.defaultConfig().build(), completionCallback);
        assertConnectorIsRunning();
        isConnectorRunning.set(true);
        waitForStreamingRunning(null);

        // Connector receives a row whose column name is not valid, task should fail
        TestHelper.execute("ALTER TABLE numeric_table ADD `@1` INT;");
        TestHelper.execute(INSERT_NUMERIC_TYPES_STMT);
        // Connector should still be running & retrying
        assertConnectorIsRunning();
        assertTrue("The task is expected to keep retrying and not complete", isConnectorRunning.get());
        stopConnector();
        assertFalse("The connector should be stopped now", isConnectorRunning.get());
        assertThat(logInterceptor.containsErrorMessage("Illegal prefix '@' for column: @1")).isTrue();
    }

    @Test
    @FixFor("DBZ-2852")
    public void shouldTaskFailIfUsernamePasswordInvalid() throws InterruptedException {
        Configuration.Builder configBuilder = TestHelper
                .defaultConfig()
                .with(VitessConnectorConfig.VTGATE_USER, "incorrect_username")
                .with(VitessConnectorConfig.VTGATE_PASSWORD, "incorrect_password");

        Map<String, Object> result = new HashMap<>();
        start(VitessConnector.class, configBuilder.build(), (success, message, error) -> {
            result.put("success", success);
            result.put("message", message);
            result.put("error", error);
        });

        assertEquals(false, result.get("success"));
        assertThat(result.get("message").toString().contains("Connector configuration is not valid. Unable to connect: "));
        assertEquals(null, result.get("error"));
    }

    @Test
    @FixFor("DBZ-2851")
    public void shouldSanitizeFieldNames() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        TestHelper.execute("ALTER TABLE numeric_table ADD `-foo-` INT default 10;");

        startConnector(builder -> builder.with(CommonConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, "avro"), false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        final List<SchemaAndValueField> fields = schemasAndValuesForNumericTypes();
        fields.add(new SchemaAndValueField("_foo_", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        assertInsert(INSERT_NUMERIC_TYPES_STMT, fields, TestHelper.PK_FIELD);
    }

    @Test
    @FixFor("DBZ-2906")
    public void shouldSanitizeDecimalValue() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        TestHelper.execute("ALTER TABLE numeric_table ADD decimal_col2 DECIMAL(14, 4) DEFAULT 12.3400;");
        TestHelper.execute("ALTER TABLE numeric_table ADD decimal_col3 DECIMAL(14, 4) DEFAULT -12.3400;");

        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        final List<SchemaAndValueField> fields = schemasAndValuesForNumericTypes();
        fields.add(new SchemaAndValueField("decimal_col2", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "12.3400"));
        fields.add(new SchemaAndValueField("decimal_col3", SchemaBuilder.OPTIONAL_STRING_SCHEMA, "-12.3400"));
        assertInsert(INSERT_NUMERIC_TYPES_STMT, fields, TestHelper.PK_FIELD);
    }

    @Test
    @FixFor("DBZ-3668")
    public void shouldOutputRecordsInCloudEventsFormat() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        TestHelper.executeDDL("vitess_create_tables.ddl");

        startConnector();

        waitForGtidAcquiring(logInterceptor);

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO no_pk_table (id,int_col) values (1001, 1)");

        SourceRecord record = consumer.remove();
        CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "vitess", TEST_SERVER, false);

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO no_pk_table (id,int_col) values (1002, 2)");

        record = consumer.remove();
        CloudEventsConverterTest.shouldConvertToCloudEventsInJson(record, false, jsonNode -> {
            assertThat(jsonNode.get(CloudEventsMaker.FieldName.ID).asText()).contains("vgtid:");
        });
        CloudEventsConverterTest.shouldConvertToCloudEventsInJsonWithDataAsAvro(record, false);
        CloudEventsConverterTest.shouldConvertToCloudEventsInAvro(record, "vitess", TEST_SERVER, false);
    }

    @Test
    public void testNoPerTaskOffsetStorage() throws Exception {
        testOffsetStorage(false);
    }

    @Test
    public void testPerTaskOffsetStorage() throws Exception {
        testOffsetStorage(true);
    }

    @Test
    public void testTableIncludeFilter() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "numeric_table";
        startConnector(Function.identity(), false, false, 1, -1, -1, tableInclude, "");
        assertConnectorIsRunning();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        // We should not receive record from string_table
        TestHelper.execute(INSERT_STRING_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);
        // We should receive record from numeeric_table
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    @Test
    public void testGetVitessShards() throws Exception {
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        Set<String> shards = new HashSet<>(VitessConnector.getVitessShards(config));
        assertEquals(new HashSet<>(Arrays.asList("0")), shards);
    }

    @Test
    public void testGetKeyspaceTables() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        Set<String> tables = new HashSet<>(VitessConnector.getKeyspaceTables(config));
        // Remove system tables starts with _
        tables = tables.stream().filter(t -> !t.startsWith("_")).collect(Collectors.toSet());
        List<String> expectedTables = Arrays.asList(
                "numeric_table", "string_table", "character_set_collate_table", "enum_table", "set_table", "time_table",
                "no_pk_table", "pk_single_unique_key_table", "no_pk_multi_unique_keys_table",
                "no_pk_multi_comp_unique_keys_table", "comp_pk_table");
        Set<String> expectedTablesHashSet = new HashSet<>(expectedTables);
        assertTrue(tables.containsAll(expectedTablesHashSet));
    }

    @Test
    public void testCopyAndReplicateTable() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "numeric_table";
        startConnector(Function.identity(), false, false, 1, -1, -1, tableInclude, null, TestHelper.TEST_SHARD);

        // We should receive a record written before starting the connector.
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
        assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, "numeric_table");
        assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);

        // We should receive additional record from numeric_table
        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    @Test
    public void testVgtidIncludesLastPkDuringTableCopy() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        int expectedSnapshotRecordsCount = 10;
        final String tableName = "numeric_table";
        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);
        }
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + tableName + "," + TEST_UNSHARDED_KEYSPACE + "." + tableName;
        startConnector(Function.identity(), false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.INITIAL, TestHelper.TEST_SHARD);

        // We should receive a record written before starting the connector.
        consumer = testConsumer(expectedSnapshotRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, tableName);
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);

            if (i == expectedSnapshotRecordsCount) {
                Map<String, ?> prevOffset = record.sourceOffset();
                Map<String, ?> prevPartition = record.sourcePartition();
                Testing.print(String.format("Offset: %s, partition: %s", prevOffset, prevPartition));
                final String vgtidStr = (String) prevOffset.get(SourceInfo.VGTID_KEY);
                final String expectedJSONString = "[{\"keyspace\":\"test_unsharded_keyspace\",\"shard\":\"0\"," +
                        "\"gtid\":\"MySQL56/6a18875e-6d37-11ee-ac9a-0242ac110002:1-224\"," +
                        "\"table_p_ks\":[{\"table_name\":\"numeric_table\",\"lastpk\":" +
                        "{\"fields\":[{\"name\":\"id\",\"type\":\"INT64\",\"charset\":63,\"flags\":49667}]," +
                        "\"rows\":[{\"lengths\":[\"2\"],\"values\":\"10\"}]}}]}]";
                Vgtid actualVgtid = Vgtid.of(vgtidStr);
                Vgtid expectedVgtid = Vgtid.of(expectedJSONString);
                assertThat(actualVgtid.getShardGtids().size()).isEqualTo(1);
                assertThat(actualVgtid.getShardGtids().get(0).getTableLastPrimaryKeys()).isEqualTo(
                        expectedVgtid.getShardGtids().get(0).getTableLastPrimaryKeys());
            }
        }
    }

    @Test
    public void testMidSnapshotRecoveryLargeTable() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        int expectedSnapshotRecordsCount = 10000;
        String rowValue = "(1, 1, 12, 12, 123, 123, 1234, 1234, 12345, 12345, 18446744073709551615, 1.5, 2.5, 12.34, true)";
        String tableName = "numeric_table";
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
        for (int i = 1; i < expectedSnapshotRecordsCount; i++) {
            insertRows.append(", ").append(rowValue);
        }

        String insertRowsStatement = insertRows.toString();
        TestHelper.execute(insertRowsStatement);

        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + tableName;
        startConnector(Function.identity(), false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.INITIAL, TestHelper.TEST_SHARD);

        consumer = testConsumer(1, tableInclude);
        consumer.await(TestHelper.waitTimeForRecords(), 0, TimeUnit.SECONDS);
        stopConnector();
        // Upper bound is the total size of the table so set that to prevent early termination
        consumer = testConsumer(expectedSnapshotRecordsCount, tableInclude);
        int recordCount = consumer.countRecords(5, TimeUnit.SECONDS);
        // Assert snapshot is partially complete
        assertThat(recordCount).isPositive();
        assertThat(recordCount < expectedSnapshotRecordsCount).isTrue();
        // Assert the total snapshot records are sent after starting
        consumer = testConsumer(expectedSnapshotRecordsCount, tableInclude);
        startConnector();
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            assertRecordInserted(TEST_UNSHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD, Long.valueOf(i));
        }
        assertNoRecordsToConsume();
    }

    @Test
    public void testResumeSnapshotOnLastPkSingleTable() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        int totalRecordsInTable = 10;
        for (int i = 1; i <= totalRecordsInTable; i++) {
            TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);
        }
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "numeric_table";
        startConnector((builder) -> builder.with(
                VitessConnectorConfig.VGTID,
                "[{\"keyspace\":\"test_unsharded_keyspace\",\"shard\":\"0\"," +
                        "\"gtid\":\"current\"," +
                        "\"table_p_ks\":[{\"table_name\":\"numeric_table\",\"lastpk\":{\"fields\":" +
                        "[{\"name\":\"id\",\"type\":\"INT64\",\"charset\":63,\"flags\":49667}]," +
                        "\"rows\":[{\"lengths\":[\"1\"],\"values\":\"5\"}]}}]}]"),
                false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.NEVER, TestHelper.TEST_SHARD);

        // We trigger a snapshot, but the previous GTID (specified in config) has a primary key value
        // So we only expect the total records in the table (10) minus the Primary Key value (5) = 5
        // records in total. Primary key value indicates the last primary key streamed.
        int expectedSnapshotRecordsCount = 5;
        consumer = testConsumer(expectedSnapshotRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, "numeric_table");
            assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);
        }

        // We should receive additional record from numeric_table
        int expectedStreamingRecordCount = 1;
        consumer.expects(expectedStreamingRecordCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    @Test
    public void testCopyNoRecordsAndReplicateTable() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");

        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "numeric_table";
        // An exception due to duplicate BEGIN events (Buffered event type: BEGIN, FIELD, VGTID) shouldn't occur
        startConnector(Function.identity(), false, false, 1, -1, -1, tableInclude, null, null);

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        // We should receive record from numeric_table
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    @Test
    public void testInitialSnapshotModeHaveMultiShard() throws Exception {
        final boolean hasMultipleShards = true;

        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_SHARDED_KEYSPACE);

        startConnector(Function.identity(), hasMultipleShards, false, 1, -1, -1, null, null, null);

        // We should receive a record written before starting the connector.
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT, TEST_SHARDED_KEYSPACE), TestHelper.PK_FIELD);
        assertSourceInfo(record, TEST_SERVER, TEST_SHARDED_KEYSPACE, "numeric_table");
        assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);

        // We should receive additional record from numeric_table
        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TEST_SHARDED_KEYSPACE, TestHelper.PK_FIELD, hasMultipleShards);
    }

    @Test
    public void testCopyTableAndRestart() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);

        String tableInclude = TEST_UNSHARDED_KEYSPACE + "\\." + "numeric_table";

        // An exception due to duplicate BEGIN events (Buffered event type: BEGIN, FIELD) shouldn't occur
        startConnector(Function.identity(), false, false, 1, -1, -1, tableInclude, null, null);

        // We should receive a record written before starting the connector.
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
        assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, "numeric_table");
        assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);

        // Restart the connector.
        stopConnector();
        startConnector(Function.identity(), false, false, 1, -1, -1, tableInclude, null, null);

        // We shouldn't receive a record written before restarting the connector.
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    @Test
    public void testCopyAndReplicatePerTaskOffsetStorage() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        TestHelper.execute(INSERT_NUMERIC_TYPES_STMT, TEST_UNSHARDED_KEYSPACE);
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + "numeric_table";
        startConnector(Function.identity(), false, true, 1, 0, 1, tableInclude, null, null);

        // We should receive a record written before starting the connector.
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_NUMERIC_TYPES_STMT), TestHelper.PK_FIELD);
        assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, "numeric_table");
        assertRecordSchemaAndValues(schemasAndValuesForNumericTypes(), record, Envelope.FieldName.AFTER);

        // We should receive additional record from numeric_table
        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_NUMERIC_TYPES_STMT, schemasAndValuesForNumericTypes(), TestHelper.PK_FIELD);
    }

    private void testOffsetStorage(boolean offsetStoragePerTask) throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_UNSHARDED_KEYSPACE);

        boolean hasMultipleShards = false;
        final int numTasks = 1;
        final int gen = 0;
        final int tid = 0;
        Configuration config = TestHelper.defaultConfig(hasMultipleShards,
                offsetStoragePerTask, numTasks, gen, 1, null, VitessConnectorConfig.SnapshotMode.NEVER,
                TestHelper.TEST_SHARD).build();
        final String serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
        Map<String, String> srcPartition = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, serverName);
        if (offsetStoragePerTask) {
            srcPartition.put(VitessPartition.TASK_KEY_PARTITION_KEY, VitessConnector.getTaskKeyName(tid, numTasks, gen));
        }

        startConnector(Function.identity(), hasMultipleShards, offsetStoragePerTask, numTasks, gen, 1, null, "");

        consumer = testConsumer(1);
        executeAndWait("INSERT INTO pk_single_unique_key_table (id, int_col) VALUES (1, 1);",
                TEST_UNSHARDED_KEYSPACE);

        SourceRecord record = consumer.remove();
        Map<String, ?> prevOffset = record.sourceOffset();
        Map<String, ?> prevPartition = record.sourcePartition();
        Testing.print(String.format("Offset: %s, partition: %s", prevOffset, prevPartition));
        final String vgtidStr = (String) prevOffset.get(SourceInfo.VGTID_KEY);
        Vgtid prevVgtid = Vgtid.of(vgtidStr);
        assertEquals(prevVgtid.getShardGtids().size(), 1);
        assertEquals(prevPartition, srcPartition);

        stopConnector();

        VitessOffsetContext.Loader loader = new VitessOffsetContext.Loader(
                new VitessConnectorConfig(Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, serverName)
                        .build()));
        Map<String, String> partition = new VitessPartition(serverName,
                offsetStoragePerTask ? VitessConnector.getTaskKeyName(0, numTasks, gen) : null).getSourcePartition();
        Map<String, ?> lastCommittedOffset = readLastCommittedOffset(config, partition);
        VitessOffsetContext offsetContext = loader.load(lastCommittedOffset);
        Vgtid restartVgtid = offsetContext.getRestartVgtid();
        Testing.print(String.format("task: %d, Offset: %s", tid, lastCommittedOffset));
        Testing.print(String.format("task: %d, vgtid: %s", tid, prevVgtid));
        assertEquals(prevOffset, lastCommittedOffset);
        assertEquals(prevPartition, partition);
        assertEquals(prevVgtid, restartVgtid);

        Testing.print("*** Done with verifying without offset.storage.per.task");
    }

    private void waitForGtidAcquiring(final LogInterceptor logInterceptor) {
        // The inserts must happen only after GTID to stream from is obtained
        Awaitility.await().atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .until(() -> logInterceptor.containsMessage("set to the GTID current for keyspace"));
    }

    private void waitForShardedGtidAcquiring(final LogInterceptor logInterceptor) {
        // The inserts must happen only after GTID to stream from is obtained
        Awaitility.await().atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .until(() -> logInterceptor.containsMessage("Default VGTID '[{\"keyspace\":"));
    }

    private void waitForVStreamStarted(final LogInterceptor logInterceptor) {
        // The inserts must happen only after VStream is started with some buffer time.
        Awaitility.await().atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> logInterceptor.containsMessage("Started VStream"));
    }

    private void startConnector() throws InterruptedException {
        startConnector(false, TestHelper.TEST_SHARD);
    }

    /**
     * Start the connector.
     *
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @throws InterruptedException
     */
    private void startConnector(boolean hasMultipleShards) throws InterruptedException {
        startConnector(Function.identity(), hasMultipleShards, "");
    }

    private void startConnector(boolean hasMultipleShards, String shards) throws InterruptedException {
        startConnector(Function.identity(), hasMultipleShards, shards);
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig,
                                boolean hasMultipleShards)
            throws InterruptedException {
        startConnector(customConfig, hasMultipleShards, false, 1, -1, -1, null, VitessConnectorConfig.SnapshotMode.NEVER, "");
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig,
                                boolean hasMultipleShards, String shards)
            throws InterruptedException {
        startConnector(customConfig, hasMultipleShards, false, 1, -1, -1, null, VitessConnectorConfig.SnapshotMode.NEVER, shards);
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig,
                                boolean hasMultipleShards, boolean offsetStoragePerTask,
                                int numTasks, int gen, int prevNumTasks, String tableInclude, String shards)
            throws InterruptedException {
        startConnector(customConfig, hasMultipleShards, offsetStoragePerTask, numTasks, gen, prevNumTasks,
                tableInclude, VitessConnectorConfig.SnapshotMode.NEVER, shards);
    }

    private void startConnector(Function<Configuration.Builder, Configuration.Builder> customConfig,
                                boolean hasMultipleShards, boolean offsetStoragePerTask,
                                int numTasks, int gen, int prevNumTasks, String tableInclude,
                                VitessConnectorConfig.SnapshotMode snapshotMode, String shards)
            throws InterruptedException {
        Configuration.Builder configBuilder = customConfig.apply(TestHelper.defaultConfig(
                hasMultipleShards, offsetStoragePerTask, numTasks, gen, prevNumTasks, tableInclude, snapshotMode, shards));
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        start(VitessConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        String taskId = offsetStoragePerTask ? VitessConnector.getTaskKeyName(0, 1, gen) : null;
        waitForStreamingRunning(taskId);
        waitForVStreamStarted(logInterceptor);
    }

    private void waitForStreamingRunning(String taskId) throws InterruptedException {
        waitForStreamingRunning(taskId, Module.name(), TEST_SERVER);
    }

    private SourceRecord assertInsert(
                                      String statement,
                                      List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                      String pkField) {
        return assertInsert(statement, expectedSchemaAndValuesByColumn, TEST_UNSHARDED_KEYSPACE, pkField, false);
    }

    /**
     * Assert that the connector receives a valid insert event.
     *
     * @param statement The insert sql statement
     * @param expectedSchemaAndValuesByColumn The expected column type and value
     * @param pkField The primary key column's name
     * @param hasMultipleShards whether the keyspace has multiple shards
     * @return The {@link SourceRecord} generated from the insert event
     */
    private SourceRecord assertInsert(
                                      String statement,
                                      List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                      String keyspace,
                                      String pkField,
                                      boolean hasMultipleShards) {
        TableId table = tableIdFromInsertStmt(statement, keyspace);

        try {
            executeAndWait(statement, keyspace);
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(statement, keyspace), pkField);
            assertRecordOffset(record, hasMultipleShards);
            assertSourceInfo(record, TEST_SERVER, keyspace, table.table());
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
        return assertRecordInserted(expectedTopicName, pkField, null);
    }

    private SourceRecord assertRecordInserted(String expectedTopicName, String pkField, Object pkValue) {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord insertedRecord = consumer.remove();
        return assertRecordInserted(insertedRecord, expectedTopicName, pkField, pkValue);
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
        return assertRecordInserted(insertedRecord, expectedTopicName, pkField, null);
    }

    private SourceRecord assertRecordInserted(SourceRecord insertedRecord, String expectedTopicName, String pkField, Object pkValue) {
        assertEquals(topicName(expectedTopicName), insertedRecord.topic());
        if (pkField != null) {
            VitessVerifyRecord.isValidInsert(insertedRecord, pkField);
        }
        else {
            VerifyRecord.isValidInsert(insertedRecord);
        }
        if (pkValue != null) {
            VitessVerifyRecord.isValidInsert(insertedRecord, pkField, pkValue);
        }
        return insertedRecord;
    }

    private SourceRecord assertRecordUpdated(SourceRecord updatedRecord) {
        VerifyRecord.isValidUpdate(updatedRecord);
        return updatedRecord;
    }

    private SourceRecord assertRecordBeginSourceRecord() {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord record = consumer.remove();
        return record;
    }

    /**
     * Assert that the connector receives a valid BEGIN event.
     *
     * @return The transaction id
     */
    private String assertRecordBegin() {
        SourceRecord record = assertRecordBeginSourceRecord();
        final Struct end = (Struct) record.value();
        assertThat(end.getString("status")).isEqualTo("BEGIN");
        return end.getString("id");
    }

    /**
     * Assert that the connector receives a valid END event.
     *
     * @param expectedTxId The expected transaction id
     * @param expectedEventCount The expected event count
     */
    private void assertRecordEnd(String expectedTxId, long expectedEventCount) {
        assertFalse("records not generated", consumer.isEmpty());
        SourceRecord record = consumer.remove();
        final Struct end = (Struct) record.value();
        assertThat(end.getString("status")).isEqualTo("END");
        assertThat(end.getString("id")).isEqualTo(expectedTxId);
        assertThat(end.getInt64("event_count")).isEqualTo(expectedEventCount);
    }

    private void executeAndWait(String statement) throws Exception {
        executeAndWait(statement, TEST_UNSHARDED_KEYSPACE);
    }

    private void executeAndWait(String statement, String database) throws Exception {
        executeAndWait(Collections.singletonList(statement), database);
    }

    private void executeAndWait(List<String> statements) throws Exception {
        executeAndWait(statements, TEST_UNSHARDED_KEYSPACE);
    }

    private void executeAndWait(List<String> statements, String database) throws Exception {
        TestHelper.execute(statements, database);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
    }

    private static String topicName(String suffix) {
        return TEST_SERVER + "." + suffix;
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
        if (expected.type() == Type.CLASS) {
            assertThat(((Class<?>) key.defaultValue).getName()).isEqualTo((String) expected.defaultValue());
        }
        else if (expected.type() == ConfigDef.Type.LIST && key.defaultValue != null) {
            assertThat(key.defaultValue).isEqualTo(Arrays.asList(expected.defaultValue()));
        }
        else {
            assertThat(key.defaultValue).isEqualTo(expected.defaultValue());
        }
        assertThat(key.dependents).isEqualTo(expected.dependents());
        assertThat(key.width).isNotNull();
        assertThat(key.group).isNotNull();
        assertThat(key.orderInGroup).isGreaterThan(0);
        assertThat(key.validator).isNull();
        assertThat(key.recommender).isNull();
    }
}
