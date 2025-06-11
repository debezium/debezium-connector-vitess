/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.connector.vitess.connection.VStreamOutputReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.vitess.proto.Query;

public class VitessBigIntUnsignedTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordEmitterTest.class);

    @Before
    public void setUp() {
        DebeziumOpenLineageEmitter.init(TestHelper.defaultConfig().build().asMap(), "test_server");
    }

    protected static Object getJavaValue(VitessConnectorConfig.BigIntUnsignedHandlingMode mode) {
        if (mode == null) {
            return "23";
        }
        switch (mode) {
            case LONG:
                return 23L;
            case STRING:
                return "23";
            case PRECISE:
                return new BigDecimal("23");
            default:
                throw new IllegalArgumentException("Unknown bigIntUnsignedHandlingMode: " + mode);
        }
    }

    protected static List<TestHelper.ColumnValue> defaultColumnValues(VitessConnectorConfig.BigIntUnsignedHandlingMode mode) {
        return Arrays.asList(
                new TestHelper.ColumnValue("bool_col", Query.Type.INT8, Types.SMALLINT, "1".getBytes(), (short) 1),
                new TestHelper.ColumnValue("int_col", Query.Type.INT32, Types.INTEGER, null, null),
                new TestHelper.ColumnValue("bigint_unsigned_col", Query.Type.UINT64, Types.BIGINT, "23".getBytes(), getJavaValue(mode)),
                new TestHelper.ColumnValue("string_col", Query.Type.VARCHAR, Types.VARCHAR, "test".getBytes(), "test"));
    }

    protected static List<ReplicationMessage.Column> defaultRelationMessageColumns(VitessConnectorConfig.BigIntUnsignedHandlingMode mode) {
        return defaultColumnValues(mode).stream()
                .map(x -> x.getReplicationMessageColumn())
                .collect(Collectors.toList());
    }

    protected static Struct defaultStruct(TableSchema tableSchema, VitessConnectorConfig.BigIntUnsignedHandlingMode mode) {
        Struct struct = new Struct(tableSchema.valueSchema());
        for (TestHelper.ColumnValue columnValue : defaultColumnValues(mode)) {
            struct.put(columnValue.getReplicationMessageColumn().getName(), columnValue.getJavaValue());
        }
        return struct;
    }

    protected void handleInsert(VitessConnectorConfig.BigIntUnsignedHandlingMode mode) throws Exception {
        VitessConnectorConfig connectorConfig;
        VitessDatabaseSchema schema;
        VStreamOutputMessageDecoder decoder;

        Configuration.Builder builder = TestHelper.defaultConfig();
        Configuration config = mode == null ? builder.build()
                : builder.with(VitessConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE.name(), mode.getValue()).build();
        connectorConfig = new VitessConnectorConfig(config);
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(schema);
        // initialize schema by FIELD event
        decoder.processMessage(TestHelper.newFieldEvent(defaultColumnValues(mode)), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());
        assertThat(table.columnWithName("bigint_unsigned_col").jdbcType() == Types.BIGINT);

        // setup fixture
        ReplicationMessage message = new VStreamOutputReplicationMessage(
                ReplicationMessage.Operation.INSERT,
                AnonymousValue.getInstant(),
                AnonymousValue.getString(),
                AnonymousValue.getString(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                TestHelper.TEST_SHARD,
                null,
                defaultRelationMessageColumns(mode));

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
        Object[] newColumnValues = emitter.getNewColumnValues();
        TableSchema tableSchema = schema.schemaFor(table.id());
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        assertThat(newValue).isEqualTo(defaultStruct(tableSchema, mode));
    }

    @Test
    public void testHandlingModeString() throws Exception {
        handleInsert(VitessConnectorConfig.BigIntUnsignedHandlingMode.STRING);
    }

    @Test
    public void testHandlingModePrecise() throws Exception {
        handleInsert(VitessConnectorConfig.BigIntUnsignedHandlingMode.PRECISE);
    }

    @Test
    public void testHandlingModeLong() throws Exception {
        handleInsert(VitessConnectorConfig.BigIntUnsignedHandlingMode.LONG);
    }

    @Test
    public void testHandlingModeDefault() throws Exception {
        handleInsert(null);
    }

    private VitessPartition initializePartition() {
        return new VitessPartition("test", null);
    }
}
