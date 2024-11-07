/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_EMPTY_SHARD_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.TEST_NON_EMPTY_SHARD;
import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD1;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD1_EPOCH;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD2;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD2_EPOCH;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.TEST_SHARD_TO_EPOCH;
import static io.debezium.connector.vitess.TestHelper.TEST_UNSHARDED_KEYSPACE;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_TEMPLATE;
import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
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
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.connector.vitess.pipeline.txmetadata.ShardEpochMap;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessRankProvider;
import io.debezium.connector.vitess.transforms.RemoveField;
import io.debezium.converters.CloudEventsConverterTest;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
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
        assertInsert(INSERT_SET_TYPE_STMT, schemasAndValuesForSetType(), TestHelper.PK_FIELD);
    }

    @Test
    @FixFor("DBZ-7962")
    public void shouldReceiveHeartbeatEvents() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(
                Heartbeat.HEARTBEAT_INTERVAL.name(), 1000),
                false);
        assertConnectorIsRunning();

        String topic = Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString() + "." + TEST_SERVER;
        int expectedHeartbeatRecords = 1;
        Awaitility
                .await()
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> consumeRecordsByTopic(expectedHeartbeatRecords).allRecordsInOrder().size() >= expectedHeartbeatRecords);

        AbstractConnectorTest.SourceRecords records = consumeRecordsByTopic(expectedHeartbeatRecords, 1);
        assertThat(records.recordsForTopic(topic).size()).isEqualTo(expectedHeartbeatRecords);
    }

    @Test
    @FixFor("DBZ-7962")
    public void shouldReceiveHeartbeatEventsShardedKeyspace() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        String expectedShard = "-80";
        startConnector(config -> config.with(
                Heartbeat.HEARTBEAT_INTERVAL.name(), 1000).with(
                        VitessConnectorConfig.SHARD, expectedShard),
                true);
        assertConnectorIsRunning();

        String topic = Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString() + "." + TEST_SERVER;
        int expectedHeartbeatRecords = 1;
        Awaitility
                .await()
                .atMost(Duration.ofSeconds(TestHelper.waitTimeForRecords()))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> consumeRecordsByTopic(expectedHeartbeatRecords).allRecordsInOrder().size() >= expectedHeartbeatRecords);

        AbstractConnectorTest.SourceRecords records = consumeRecordsByTopic(expectedHeartbeatRecords, 1);
        List<SourceRecord> recordsForTopic = records.recordsForTopic(topic);
        assertThat(recordsForTopic.size()).isEqualTo(expectedHeartbeatRecords);
        Struct value = (Struct) recordsForTopic.get(0).value();
        Vgtid vgtid = Vgtid.of(value.getString("vgtid"));
        assertThat(vgtid.getShardGtids().size()).isEqualTo(1);
        assertThat(vgtid.getShardGtids().get(0).getShard()).isEqualTo(expectedShard);
    }

    @Test
    @FixFor("DBZ-8325")
    public void shouldReceiveSchemaChangeEventAfterDataChangeEvent() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        // startConnector();
        startConnector(config -> config
                .with(CommonConnectorConfig.INCLUDE_SCHEMA_CHANGES, true),
                false);
        assertConnectorIsRunning();

        String schemaChangeTopic = TestHelper.defaultConfig().build().getString(CommonConnectorConfig.TOPIC_PREFIX);
        String dataChangeTopic = String.join(".",
                TestHelper.defaultConfig().build().getString(CommonConnectorConfig.TOPIC_PREFIX),
                TEST_UNSHARDED_KEYSPACE,
                "ddl_table");

        String ddl = "ALTER TABLE ddl_table ADD COLUMN new_column_name INT";
        TestHelper.execute("INSERT INTO ddl_table (id, int_unsigned_col, json_col) VALUES (1, 2, '{\"1\":2}');");
        TestHelper.execute(ddl);

        int expectedDataChangeRecords = 1;
        int expectedSchemaChangeRecords = 1;
        int expectedTotalRecords = expectedDataChangeRecords + expectedSchemaChangeRecords;
        consumer = testConsumer(expectedTotalRecords);
        consumer.expects(expectedTotalRecords);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 0; i < expectedTotalRecords; i++) {
            SourceRecord record = consumer.remove();
            Struct value = (Struct) record.value();
            Struct source = (Struct) value.get("source");
            assertThat(source.getString("table")).isEqualTo("ddl_table");
            assertThat(source.getString("shard")).isEqualTo(TEST_SHARD);
            if (i == 1) {
                assertThat(record.topic()).isEqualTo(schemaChangeTopic);
                assertThat(value.getString("ddl")).isEqualToIgnoringCase(ddl);
            }
            else {
                assertThat(record.topic()).isEqualTo(dataChangeTopic);
            }
        }
        assertThat(consumer.isEmpty());
    }

    @Test
    @FixFor("DBZ-8325")
    public void shouldReceiveSchemaEventsShardedBeforeAnyDataEvents() throws Exception {
        String keyspace = TEST_SHARDED_KEYSPACE;
        String table = keyspace + ".ddl_table";
        TestHelper.executeDDL("vitess_create_tables.ddl", keyspace);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(VitessConnectorConfig.INCLUDE_SCHEMA_CHANGES, true),
                true);
        assertConnectorIsRunning();

        String schemaChangeTopic = TestHelper.defaultConfig().build().getString(CommonConnectorConfig.TOPIC_PREFIX);

        String addCol = "ALTER TABLE ddl_table ADD COLUMN new_column_name INT";
        String addPartition = "ALTER TABLE ddl_table ADD PARTITION (PARTITION p2 VALUES LESS THAN (2000))";
        String dropPartition = "ALTER TABLE ddl_table DROP PARTITION p0";
        String truncateTable = "TRUNCATE TABLE ddl_table";
        // Put in the fully qualified table name (with keyspace) to ensure we can parse the table name fine
        String dropTable = "DROP TABLE test_sharded_keyspace.ddl_table";
        String createTable = "CREATE TABLE test_sharded_keyspace.ddl_table (id BIGINT NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))";
        TestHelper.execute(addCol, TEST_SHARDED_KEYSPACE);
        TestHelper.execute(addPartition, TEST_SHARDED_KEYSPACE);
        TestHelper.execute(dropPartition, TEST_SHARDED_KEYSPACE);
        TestHelper.execute(truncateTable, TEST_SHARDED_KEYSPACE);
        TestHelper.execute(dropTable, TEST_SHARDED_KEYSPACE);
        TestHelper.execute(createTable, TEST_SHARDED_KEYSPACE);

        // 6 for the changes above
        // 2 shards, so 6 * 2 = 12
        int expectedSchemaChangeRecords = 12;
        consumer = testConsumer(expectedSchemaChangeRecords);
        consumer.expects(expectedSchemaChangeRecords);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 0; i < expectedSchemaChangeRecords; i++) {
            SourceRecord record = consumer.remove();
            assertThat(record.topic()).isEqualTo(schemaChangeTopic);
            Struct value = (Struct) record.value();
            Struct source = (Struct) value.get("source");
            assertThat(source.getString("table")).isEqualTo("ddl_table");
            assertThat(source.getString("shard")).isIn(List.of(TEST_SHARD1, TEST_SHARD2));
            if (i < 2) {
                assertThat(value.getString("ddl")).isEqualToIgnoringCase(addCol);
            }
            else if (i < 4) {
                assertThat(value.getString("ddl")).isEqualToIgnoringCase(addPartition);
            }
            else if (i < 6) {
                assertThat(value.getString("ddl")).isEqualToIgnoringCase(dropPartition);
            }
            else if (i < 8) {
                assertThat(value.getString("ddl")).isEqualToIgnoringCase(truncateTable);
            }
            else if (i < 10) {
                assertThat(value.getString("ddl")).containsIgnoringCase("DROP TABLE");
            }
            else if (i < 12) {
                assertThat(value.getString("ddl")).containsIgnoringCase("CREATE TABLE");
            }
        }
        assertThat(consumer.isEmpty());
    }

    @Test
    @FixFor("DBZ-2776")
    public void shouldReceiveChangesForInsertsWithEnum() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_ENUM_TYPE_STMT, schemasAndValuesForEnumType(), TestHelper.PK_FIELD);

    }

    @Test
    @FixFor("DBZ-2776")
    public void shouldReceiveChangesForInsertsWithAmbiguous() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_ENUM_AMBIGUOUS_TYPE_STMT, schemasAndValuesForEnumTypeAmbiguous(), TestHelper.PK_FIELD);

    }

    @Test
    public void shouldReceiveChangesForInsertsWithTimestampTypes() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_TIME_TYPES_STMT, schemasAndValuesForTimeType(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveChangesForInsertsWithTimestampTypesConnect() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(
                VitessConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT),
                false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_TIME_TYPES_STMT, schemasAndValuesForTimeTypeConnect(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveChangesForInsertsWithTimestampTypesPrecision() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector();
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_PRECISION_TIME_TYPES_STMT, schemasAndValuesForTimeTypePrecision(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldReceiveChangesForInsertsWithTimestampTypesPrecisionConnect() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(config -> config.with(
                VitessConnectorConfig.TIME_PRECISION_MODE, TemporalPrecisionMode.CONNECT),
                false);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_PRECISION_TIME_TYPES_STMT, schemasAndValuesForTimeTypePrecisionConnect(), TestHelper.PK_FIELD);
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
    public void shouldTruncateByteArray() throws Exception {

        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(builder -> builder.with(
                "column.truncate.to.1.chars",
                TEST_UNSHARDED_KEYSPACE + ".string_table.blob_col,"
                        + TEST_UNSHARDED_KEYSPACE + ".string_table.mediumblob_col,"
                        + TEST_UNSHARDED_KEYSPACE + ".string_table.longblob_col,"
                        + TEST_UNSHARDED_KEYSPACE + ".string_table.varbinary_col,"
                        + TEST_UNSHARDED_KEYSPACE + ".string_table.binary_col"),
                false,
                false, 1, -1, -1, null,
                VitessConnectorConfig.SnapshotMode.NEVER, "");
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        assertInsert(INSERT_BYTES_TYPES_STMT, schemasAndValuesForStringTypesTruncatedBlob(), TestHelper.PK_FIELD);
    }

    @Test
    public void shouldConsumeEventsWithExcludedColumn() throws Exception {
        String columnToExlude = "mediumtext_col";
        String someColumnIncluded = "varchar_col";
        TestHelper.executeDDL("vitess_create_tables.ddl");
        startConnector(builder -> builder.with("column.exclude.list",
                TEST_UNSHARDED_KEYSPACE + ".string_table." + columnToExlude), false,
                false, 1, -1, -1, null,
                VitessConnectorConfig.SnapshotMode.NEVER, "");
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

        consumer.expects(expectedRecordsCount);
        SourceRecord record = assertInsert(INSERT_STRING_TYPES_STMT, schemasAndValuesForStringTypesExcludedColumn(), TestHelper.PK_FIELD);
        Struct value = (Struct) record.value();
        Struct after = (Struct) value.get("after");
        assertThat(after.schema().field(columnToExlude)).isNull();
        assertThat(after.schema().field(someColumnIncluded)).isNotNull();
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

        // insert 1 row
        consumer.expects(expectedRecordsCount);
        List<SchemaAndValueField> expectedSchemaAndValuesByColumn = schemasAndValuesForNumericTypes();
        expectedSchemaAndValuesByColumn.add(
                new SchemaAndValueField("foo", SchemaBuilder.OPTIONAL_INT32_SCHEMA, 10));
        SourceRecord sourceRecord2 = assertInsert(INSERT_NUMERIC_TYPES_STMT, expectedSchemaAndValuesByColumn, TestHelper.PK_FIELD);

        String expectedOffset = RecordOffset
                .fromSourceInfo(sourceRecord)
                .incrementOffset(numOfGtidsFromDdl + 1).getVgtid();
        String actualOffset = (String) sourceRecord2.sourceOffset().get(SourceInfo.VGTID_KEY);
        assertThat(actualOffset).isGreaterThanOrEqualTo(expectedOffset);
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
    public void shouldProvideOrderedTransactionMetadata() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true),
                true,
                "-80,80-");
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

        // exercise SUT
        executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
        // First transaction.
        SourceRecord beginRecord = assertRecordBeginSourceRecord();
        assertThat(beginRecord.sourceOffset()).containsKey("transaction_epoch");
        String expectedTxId1 = ((Struct) beginRecord.value()).getString("id");
        // A 0 epoch is only used by a connector that starts with a valid gtid in its config.
        // For a connector that starts with current (default) or snapshot (empty), increment epoch (in this case from 0 -> 1
        Long expectedEpoch = 1L;
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");
            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            String txId = txn.getString("id");
            assertThat(txId).isNotNull();
            assertThat(txId).isEqualTo(expectedTxId1);
            assertThat(txn.get("transaction_epoch")).isEqualTo(expectedEpoch);
            BigDecimal expectedRank = VitessRankProvider.getRank(Vgtid.of(expectedTxId1).getShardGtid(shard).getGtid());
            assertThat(txn.get("transaction_rank")).isEqualTo(expectedRank);
            Vgtid actualVgtid = Vgtid.of(txId);
            // The current vgtid is not the previous vgtid.
            assertThat(actualVgtid).isNotEqualTo(baseVgtid);
        }
        assertRecordEnd(expectedTxId1, expectedRecordsCount);
    }

    @Test
    public void shouldUseLocalVgtid() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with("transforms", "useLocalVgtid")
                .with("transforms.useLocalVgtid.type", "io.debezium.connector.vitess.transforms.UseLocalVgtid"),
                true,
                "-80,80-");
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

        // exercise SUT
        executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
        // First transaction.
        SourceRecord beginRecord = assertRecordBeginSourceRecord();
        String expectedTxId1 = ((Struct) beginRecord.value()).getString("id");
        Long expectedEpoch = 0L;
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            Vgtid sourceVgtid = Vgtid.of(source.getString("vgtid"));
            // We have two shards for multi-shard keyspace, a local vgtid should only have one shard
            assertThat(sourceVgtid.getShardGtids().size()).isEqualTo(1);
            assertThat(sourceVgtid.getShardGtids().get(0).getShard()).isEqualTo(source.getString("shard"));
        }
        assertRecordEnd(expectedTxId1, expectedRecordsCount);
    }

    @Test
    public void shouldProvideTransactionMetadataWithoutIdOrTransactionTopic() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with("transforms", "filterTransactionTopicRecords,removeField")
                .with("transforms.filterTransactionTopicRecords.type",
                        "io.debezium.connector.vitess.transforms.FilterTransactionTopicRecords")
                .with("transforms.removeField.type", "io.debezium.connector.vitess.transforms.RemoveField")
                .with("transforms.removeField." + RemoveField.FIELD_NAMES_CONF, "transaction.id")
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true),
                true,
                "-80,80-");
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

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

        // exercise SUT
        executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
        // First transaction.
        // A 0 epoch is only used by a connector that starts with a valid gtid in its config.
        // For a connector that starts with current (default) or snapshot (empty), increment epoch (in this case from 0 -> 1
        Long expectedEpoch = 1L;
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");
            String vgtid = source.getString("vgtid");
            Vgtid actualVgtid = Vgtid.of(vgtid);
            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            assertThat(txn.schema().field("id")).isNull();
            assertThat(txn.get("transaction_epoch")).isEqualTo(expectedEpoch);
            BigDecimal expectedRank = VitessRankProvider.getRank(actualVgtid.getShardGtid(shard).getGtid());
            assertThat(txn.get("transaction_rank")).isEqualTo(expectedRank);
        }
    }

    @Test
    public void shouldProvideTransactionMetadataWithoutIdOrTransactionTopicAndUseLocalVgtid() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");
        startConnector(config -> config
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with("transforms", "filterTransactionTopicRecords,removeField,useLocalVgtid")
                .with("transforms.filterTransactionTopicRecords.type",
                        "io.debezium.connector.vitess.transforms.FilterTransactionTopicRecords")
                .with("transforms.removeField.type", "io.debezium.connector.vitess.transforms.RemoveField")
                .with("transforms.removeField." + RemoveField.FIELD_NAMES_CONF, "transaction.id")
                .with("transforms.useLocalVgtid.type", "io.debezium.connector.vitess.transforms.UseLocalVgtid")
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true),
                true,
                "-80,80-");
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);

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

        // exercise SUT
        executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
        // First transaction.
        // A 0 epoch is only used by a connector that starts with a valid gtid in its config.
        // For a connector that starts with current (default) or snapshot (empty), increment epoch (in this case from 0 -> 1
        Long expectedEpoch = 1L;
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");
            Vgtid sourceVgtid = Vgtid.of(source.getString("vgtid"));
            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            assertThat(txn.schema().field("id")).isNull();
            assertThat(txn.get("transaction_epoch")).isEqualTo(expectedEpoch);
            BigDecimal expectedRank = VitessRankProvider.getRank(sourceVgtid.getShardGtid(shard).getGtid());
            assertThat(txn.get("transaction_rank")).isEqualTo(expectedRank);
            assertThat(txn.get("total_order")).isEqualTo(1L);
            // We have two shards for multi-shard keyspace, a local vgtid should only have one shard
            assertThat(sourceVgtid.getShardGtids().size()).isEqualTo(1);
            assertThat(sourceVgtid.getShardGtids().get(0).getShard()).isEqualTo(source.getString("shard"));
        }
    }

    @Test
    public void shouldIncrementEpochWhenFastForwardVgtidWithOrderedTransactionMetadata() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");

        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> srcPartition = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER);
        String currentVgtid = String.format(
                VGTID_JSON_TEMPLATE,
                TEST_SHARDED_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.CURRENT_GTID,
                TEST_SHARDED_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.CURRENT_GTID);
        Map<String, Long> shardToEpoch = Map.of(VgtidTest.TEST_SHARD, 2L, VgtidTest.TEST_SHARD2, 3L);
        Map<String, String> offsetId = Map.of(
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, mapper.writeValueAsString(shardToEpoch),
                SourceInfo.VGTID_KEY, currentVgtid);
        Map<Map<String, ?>, Map<String, ?>> offsets = Map.of(srcPartition, offsetId);
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                .with(VitessConnectorConfig.KEYSPACE, TEST_SHARDED_KEYSPACE)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(VitessConnectorConfig.SHARD, "-80,80-")
                .build();

        storeOffsets(config, offsets);

        startConnector(config);
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

        // exercise SUT
        executeAndWait(insertRowsStatement, TEST_SHARDED_KEYSPACE);
        // First transaction.
        SourceRecord beginRecord = assertRecordBeginSourceRecord();
        assertThat(beginRecord.sourceOffset()).containsKey("transaction_epoch");
        String expectedTxId1 = ((Struct) beginRecord.value()).getString("id");
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");
            Long expectedEpoch = shardToEpoch.get(shard) + 1;
            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            String txId = txn.getString("id");
            assertThat(txId).isNotNull();
            assertThat(txId).isEqualTo(expectedTxId1);
            assertThat(txn.get("transaction_epoch")).isEqualTo(expectedEpoch);
            BigDecimal expectedRank = VitessRankProvider.getRank(Vgtid.of(expectedTxId1).getShardGtid(shard).getGtid());
            assertThat(txn.get("transaction_rank")).isEqualTo(expectedRank);
            Vgtid actualVgtid = Vgtid.of(txId);
            // The current vgtid is not the previous vgtid.
            assertThat(actualVgtid).isNotEqualTo(baseVgtid);
        }
        assertRecordEnd(expectedTxId1, expectedRecordsCount);
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
                String txId = txn.getString("id");
                assertThat(txId).isNotNull();
                assertThat(txId).isEqualTo(expectedTxId1);
                Vgtid actualVgtid = Vgtid.of(txId);
                // The current vgtid is not the previous vgtid.
                assertThat(actualVgtid).isNotEqualTo(baseVgtid);
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
                Vgtid actualVgtid = Vgtid.of(txId);
                // The current vgtid is not the previous vgtid.
                assertThat(actualVgtid).isNotEqualTo(Vgtid.of(expectedTxId1));
            }
            assertRecordEnd(expectedTxId2, expectedRecordsCount2);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        Testing.Print.enable();
    }

    @Test
    public void shouldStreamFromKeyspaceWithEmptyShardsAndExplicitShardList() throws Exception {
        final boolean hasMultipleShards = false;
        Configuration.Builder configBuilder = TestHelper.defaultConfig(true, true, 1, 0, 1, null, null);
        configBuilder = configBuilder
                .with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                .with(VitessConnectorConfig.SHARD, TEST_NON_EMPTY_SHARD)
                .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true);
        VitessConnectorConfig config = new VitessConnectorConfig(configBuilder.build());

        TestHelper.executeDDL("vitess_create_tables.ddl", config, TEST_NON_EMPTY_SHARD);
        startConnector(
                builder -> builder.with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                        .with(VitessConnectorConfig.SHARD, TEST_NON_EMPTY_SHARD)
                        .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true),
                hasMultipleShards,
                true,
                1,
                0,
                1,
                null,
                null);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(
                config,
                INSERT_NUMERIC_TYPES_STMT,
                schemasAndValuesForNumericTypes(),
                TEST_EMPTY_SHARD_KEYSPACE,
                TestHelper.PK_FIELD,
                hasMultipleShards,
                TEST_NON_EMPTY_SHARD);
    }

    @Test
    public void shouldAutoFilterEmptyShardsFromKeyspace() throws Exception {
        final boolean hasMultipleShards = false;
        Configuration.Builder configBuilder = TestHelper.defaultConfig(true, true, 1, 0, 1, null, null);
        configBuilder = configBuilder
                .with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true);
        VitessConnectorConfig config = new VitessConnectorConfig(configBuilder.build());

        TestHelper.executeDDL("vitess_create_tables.ddl", config, TEST_NON_EMPTY_SHARD);
        startConnector(
                builder -> builder.with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                        .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true),
                hasMultipleShards,
                true,
                1,
                0,
                1,
                null,
                null);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(
                config,
                INSERT_NUMERIC_TYPES_STMT,
                schemasAndValuesForNumericTypes(),
                TEST_EMPTY_SHARD_KEYSPACE,
                TestHelper.PK_FIELD,
                hasMultipleShards,
                TEST_NON_EMPTY_SHARD);
    }

    @Test
    public void shouldAutoFilterEmptyShardsWithTableIncludeList() throws Exception {
        final boolean hasMultipleShards = false;
        String tableInclude = TEST_EMPTY_SHARD_KEYSPACE + ".numeric_table";
        Configuration.Builder configBuilder = TestHelper.defaultConfig(true, true, 1, 0, 1, null, null);
        configBuilder = configBuilder
                .with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude);
        VitessConnectorConfig config = new VitessConnectorConfig(configBuilder.build());

        TestHelper.executeDDL("vitess_create_tables.ddl", config, TEST_NON_EMPTY_SHARD);
        startConnector(
                builder -> builder.with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                        .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true)
                        .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude),
                hasMultipleShards,
                true,
                1,
                0,
                1,
                null,
                null);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(
                config,
                INSERT_NUMERIC_TYPES_STMT,
                schemasAndValuesForNumericTypes(),
                TEST_EMPTY_SHARD_KEYSPACE,
                TestHelper.PK_FIELD,
                hasMultipleShards,
                TEST_NON_EMPTY_SHARD);
    }

    @Test
    public void shouldAutoFilterEmptyShardsWithTableIncludeListAndShardList() throws Exception {
        final boolean hasMultipleShards = false;
        String tableInclude = TEST_EMPTY_SHARD_KEYSPACE + ".numeric_table";
        Configuration.Builder configBuilder = TestHelper.defaultConfig(true, true, 1, 0, 1, null, null);
        configBuilder = configBuilder
                .with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true)
                .with(VitessConnectorConfig.SHARD, TEST_NON_EMPTY_SHARD)
                .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude);
        VitessConnectorConfig config = new VitessConnectorConfig(configBuilder.build());

        TestHelper.executeDDL("vitess_create_tables.ddl", config, TEST_NON_EMPTY_SHARD);
        startConnector(
                builder -> builder.with(VitessConnectorConfig.KEYSPACE, TEST_EMPTY_SHARD_KEYSPACE)
                        .with(VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true)
                        .with(VitessConnectorConfig.SHARD, TEST_NON_EMPTY_SHARD)
                        .with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, tableInclude),
                hasMultipleShards,
                true,
                1,
                0,
                1,
                null,
                null);
        assertConnectorIsRunning();

        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount);
        assertInsert(
                config,
                INSERT_NUMERIC_TYPES_STMT,
                schemasAndValuesForNumericTypes(),
                TEST_EMPTY_SHARD_KEYSPACE,
                TestHelper.PK_FIELD,
                hasMultipleShards,
                TEST_NON_EMPTY_SHARD);
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
    public void shouldMaintainEpochMapWithChangeInOffsetStoragePerTask() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl", TEST_SHARDED_KEYSPACE);
        TestHelper.applyVSchema("vitess_vschema.json");

        Map<String, String> srcPartition = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER);
        String currentVgtid = String.format(
                VGTID_JSON_TEMPLATE,
                TEST_SHARDED_KEYSPACE,
                VgtidTest.TEST_SHARD,
                Vgtid.CURRENT_GTID,
                TEST_SHARDED_KEYSPACE,
                VgtidTest.TEST_SHARD2,
                Vgtid.CURRENT_GTID);
        Map<String, String> offsetId = Map.of(
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_ID, currentVgtid,
                VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, TEST_SHARD_TO_EPOCH.toString(),
                SourceInfo.VGTID_KEY, currentVgtid);
        Map<Map<String, ?>, Map<String, ?>> offsets = Map.of(srcPartition, offsetId);
        Configuration config = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                .with(VitessConnectorConfig.KEYSPACE, TEST_SHARDED_KEYSPACE)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(VitessConnectorConfig.SHARD, "-80,80-")
                .build();

        storeOffsets(config, offsets);

        startConnector(config);
        assertConnectorIsRunning();

        Vgtid baseVgtid = TestHelper.getCurrentVgtid();
        int expectedRecordsCount = 1;
        consumer = testConsumer(expectedRecordsCount + 2);

        // exercise SUT
        executeAndWait(INSERT_NUMERIC_TYPES_STMT, TEST_SHARDED_KEYSPACE);
        // First transaction.
        SourceRecord beginRecord = assertRecordBeginSourceRecord();

        ShardEpochMap beginShardToEpoch = ShardEpochMap.of((String) beginRecord.sourceOffset().get("transaction_epoch"));
        assertThat(beginShardToEpoch.get(TEST_SHARD1)).isEqualTo(TEST_SHARD1_EPOCH + 1);
        assertThat(beginShardToEpoch.get(TEST_SHARD2)).isEqualTo(TEST_SHARD2_EPOCH + 1);
        String expectedTxId = ((Struct) beginRecord.value()).getString("id");

        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");

            ShardEpochMap shardToEpoch = ShardEpochMap.of((String) record.sourceOffset().get("transaction_epoch"));
            assertThat(shardToEpoch.get(TEST_SHARD1)).isEqualTo(TEST_SHARD1_EPOCH + 1);
            assertThat(shardToEpoch.get(TEST_SHARD2)).isEqualTo(TEST_SHARD2_EPOCH + 1);

            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            Long epoch = (Long) txn.get("transaction_epoch");
            assertThat(epoch).isEqualTo(TEST_SHARD_TO_EPOCH.get(shard) + 1);

        }
        assertRecordEnd(expectedTxId, expectedRecordsCount);

        stopConnector();

        Configuration config2 = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .with(CommonConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                .with(VitessConnectorConfig.KEYSPACE, TEST_SHARDED_KEYSPACE)
                .with(CommonConnectorConfig.PROVIDE_TRANSACTION_METADATA, true)
                .with(VitessConnectorConfig.SHARD, "-80,80-")
                .with(VitessConnectorConfig.TASKS_MAX_CONFIG, 2)
                .with(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK, "true")
                .with(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN, "0")
                .with(VitessConnectorConfig.PREV_NUM_TASKS, "1")
                .build();
        startConnector(config2);
        assertConnectorIsRunning();

        consumer = testConsumer(expectedRecordsCount + 2);
        executeAndWait(INSERT_NUMERIC_TYPES_STMT, TEST_SHARDED_KEYSPACE);

        SourceRecord beginRecord2 = assertRecordBeginSourceRecord();
        assertThat(beginRecord2.sourceOffset()).containsKey("transaction_epoch");
        String expectedTxId2 = ((Struct) beginRecord2.value()).getString("id");
        for (int i = 1; i <= expectedRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(TEST_SHARDED_KEYSPACE + ".numeric_table", TestHelper.PK_FIELD);
            Struct source = (Struct) ((Struct) record.value()).get("source");
            String shard = source.getString("shard");
            Long expectedEpoch = TEST_SHARD_TO_EPOCH.get(shard) + 1;
            final Struct txn = ((Struct) record.value()).getStruct("transaction");
            String txId = txn.getString("id");
            assertThat(txId).isNotNull();
            assertThat(txId).isEqualTo(expectedTxId2);
            assertThat(txn.get("transaction_epoch")).isEqualTo(expectedEpoch);
            BigDecimal expectedRank = VitessRankProvider.getRank(Vgtid.of(expectedTxId2).getShardGtid(shard).getGtid());
            assertThat(txn.get("transaction_rank")).isEqualTo(expectedRank);
            Vgtid actualVgtid = Vgtid.of(txId);
            // The current vgtid is not the previous vgtid.
            assertThat(actualVgtid).isNotEqualTo(baseVgtid);
        }
        assertRecordEnd(expectedTxId2, expectedRecordsCount);
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
        Set<String> shards = new HashSet<>(new VitessMetadata(config).getShards());
        assertEquals(new HashSet<>(Arrays.asList("0")), shards);
    }

    @Test
    public void testGetKeyspaceTables() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        Set<String> tables = new HashSet<>(new VitessMetadata(config).getTables());
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
    public void testSnapshotForTableWithEnums() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        int expectedSnapshotRecordsCount = 10;
        int expectedStreamingRecordsCount = 1;
        int totalRecordsCount = expectedSnapshotRecordsCount + expectedStreamingRecordsCount;
        final String tableName = "enum_table";
        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            TestHelper.execute(INSERT_ENUM_TYPE_STMT, TEST_UNSHARDED_KEYSPACE);
        }
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + tableName + "," + TEST_UNSHARDED_KEYSPACE + "." + tableName;
        startConnector(Function.identity(), false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.INITIAL, TestHelper.TEST_SHARD);

        for (int i = 1; i <= expectedStreamingRecordsCount; i++) {
            TestHelper.execute(INSERT_ENUM_TYPE_STMT, TEST_UNSHARDED_KEYSPACE);
        }

        // We should receive a record written before starting the connector.
        consumer = testConsumer(totalRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 1; i <= totalRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_ENUM_TYPE_STMT), TestHelper.PK_FIELD);
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, tableName);
            assertRecordSchemaAndValues(schemasAndValuesForEnumType(), record, Envelope.FieldName.AFTER);

            if (i == expectedSnapshotRecordsCount) {
                Map<String, ?> prevOffset = record.sourceOffset();
                Map<String, ?> prevPartition = record.sourcePartition();
                Testing.print(String.format("Offset: %s, partition: %s", prevOffset, prevPartition));
                final String vgtidStr = (String) prevOffset.get(SourceInfo.VGTID_KEY);
                final String expectedJSONString = "[{\"keyspace\":\"test_unsharded_keyspace\",\"shard\":\"0\"," +
                        "\"gtid\":\"MySQL56/6a18875e-6d37-11ee-ac9a-0242ac110002:1-224\"," +
                        "\"table_p_ks\":[{\"table_name\":\"enum_table\",\"lastpk\":" +
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
    public void testSnapshotForTableWithEnumsAmbiguous() throws Exception {
        TestHelper.executeDDL("vitess_create_tables.ddl");
        int expectedSnapshotRecordsCount = 10;
        int streamingRecordsCount = 1;
        int totalRecordsCount = expectedSnapshotRecordsCount + streamingRecordsCount;
        final String tableName = "enum_ambiguous_table";
        for (int i = 1; i <= expectedSnapshotRecordsCount; i++) {
            TestHelper.execute(INSERT_ENUM_AMBIGUOUS_TYPE_STMT, TEST_UNSHARDED_KEYSPACE);
        }
        String tableInclude = TEST_UNSHARDED_KEYSPACE + "." + tableName + "," + TEST_UNSHARDED_KEYSPACE + "." + tableName;
        startConnector(Function.identity(), false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.INITIAL, TestHelper.TEST_SHARD);

        for (int i = 1; i <= streamingRecordsCount; i++) {
            TestHelper.execute(INSERT_ENUM_AMBIGUOUS_TYPE_STMT, TEST_UNSHARDED_KEYSPACE);
        }

        // We should receive a record written before starting the connector.
        consumer = testConsumer(totalRecordsCount);
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        for (int i = 1; i <= totalRecordsCount; i++) {
            SourceRecord record = assertRecordInserted(topicNameFromInsertStmt(INSERT_ENUM_AMBIGUOUS_TYPE_STMT), TestHelper.PK_FIELD);
            assertSourceInfo(record, TEST_SERVER, TEST_UNSHARDED_KEYSPACE, tableName);
            assertRecordSchemaAndValues(schemasAndValuesForEnumTypeAmbiguous(), record, Envelope.FieldName.AFTER);

            if (i == expectedSnapshotRecordsCount) {
                Map<String, ?> prevOffset = record.sourceOffset();
                Map<String, ?> prevPartition = record.sourcePartition();
                Testing.print(String.format("Offset: %s, partition: %s", prevOffset, prevPartition));
                final String vgtidStr = (String) prevOffset.get(SourceInfo.VGTID_KEY);
                final String expectedJSONString = "[{\"keyspace\":\"test_unsharded_keyspace\",\"shard\":\"0\"," +
                        "\"gtid\":\"MySQL56/6a18875e-6d37-11ee-ac9a-0242ac110002:1-224\"," +
                        "\"table_p_ks\":[{\"table_name\":\"enum_ambiguous_table\",\"lastpk\":" +
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
        startConnector(Function.identity(), false, false, 1,
                -1, -1, tableInclude, VitessConnectorConfig.SnapshotMode.INITIAL, TestHelper.TEST_SHARD);
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
        startConnector(configBuilder.build());
    }

    private void startConnector(Configuration config) throws InterruptedException {
        final LogInterceptor logInterceptor = new LogInterceptor(VitessReplicationConnection.class);
        start(VitessConnector.class, config);
        assertConnectorIsRunning();
        String taskId = config.getBoolean(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK)
                ? VitessConnector.getTaskKeyName(0, 1, config.getInteger(VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN))
                : null;
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

    private SourceRecord assertInsert(
                                      VitessConnectorConfig config,
                                      String statement,
                                      List<SchemaAndValueField> expectedSchemaAndValuesByColumn,
                                      String keyspace,
                                      String pkField,
                                      boolean hasMultipleShards,
                                      String shardToQuery) {
        try {
            TableId table = tableIdFromInsertStmt(statement, keyspace);
            if (config.excludeEmptyShards() && shardToQuery != null) {
                new VitessMetadata(config).executeQuery(statement, shardToQuery);
            }
            else {
                executeAndWait(statement);
            }
            consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
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
