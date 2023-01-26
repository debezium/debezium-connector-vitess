/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.connector.vitess.connection.VStreamOutputReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import io.vitess.proto.Query;

public class VitessBigIntUnsignedTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordEmitterTest.class);

    private static VitessConnectorConfig connectorConfig;
    private static VitessDatabaseSchema schema;
    private static VStreamOutputMessageDecoder decoder;

    public static List<TestHelper.ColumnValue> defaultColumnValues() {
        return Arrays.asList(
                new TestHelper.ColumnValue("bool_col", Query.Type.INT8, Types.SMALLINT, "1".getBytes(), (short) 1),
                new TestHelper.ColumnValue("int_col", Query.Type.INT32, Types.INTEGER, null, null),
                new TestHelper.ColumnValue("bigint_unsigned_col", Query.Type.UINT64, Types.BIGINT, "23".getBytes(), 23L),
                new TestHelper.ColumnValue("string_col", Query.Type.VARBINARY, Types.VARCHAR, "test".getBytes(), "test"));
    }

    public static List<ReplicationMessage.Column> defaultRelationMessageColumns() {
        return defaultColumnValues().stream()
                .map(x -> x.getReplicationMessageColumn())
                .collect(Collectors.toList());
    }

    public static List<Object> defaultJavaValues() {
        return defaultColumnValues().stream().map(x -> x.getJavaValue()).collect(Collectors.toList());
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        Configuration config = TestHelper.defaultConfig().with(
                VitessConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE.name(),
                VitessConnectorConfig.BigIntUnsignedHandlingMode.LONG.getValue()).build();
        connectorConfig = new VitessConnectorConfig(config);
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(connectorConfig, schema);
        // initialize schema by FIELD event
        decoder.processMessage(TestHelper.newFieldEvent(defaultColumnValues()), null, null, false);
        Table table = schema.tableFor(new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE));
        assertThat(table.columnWithName("bigint_unsigned_col").jdbcType() == Types.BIGINT);
    }

    @Test
    public void shouldGetNewColumnValuesFromInsert() {
        // setup fixture
        ReplicationMessage message = new VStreamOutputReplicationMessage(
                ReplicationMessage.Operation.INSERT,
                AnonymousValue.getInstant(),
                AnonymousValue.getString(),
                new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE)
                        .toDoubleQuotedString(),
                null,
                defaultRelationMessageColumns());

        // exercise SUT
        VitessChangeRecordEmitter emitter = new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                connectorConfig,
                schema,
                message);

        // verify outcome
        assertThat(emitter.getOperation()).isEqualTo(Envelope.Operation.CREATE);
        assertThat(emitter.getOldColumnValues()).isNull();
        assertThat(emitter.getNewColumnValues()).isEqualTo(defaultJavaValues().toArray());
    }

    private VitessPartition initializePartition() {
        return new VitessPartition("test", null);
    }
}
