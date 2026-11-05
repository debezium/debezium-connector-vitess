/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Types;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.VitessTaskContext;
import io.debezium.connector.vitess.VitessValueConverter;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.vitess.proto.Query;

import binlogdata.Binlogdata;

public class VStreamOutputMessageDecoderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VStreamOutputMessageDecoderTest.class);

    private VitessConnectorConfig connectorConfig;
    private VitessDatabaseSchema schema;
    private VStreamOutputMessageDecoder decoder;

    @BeforeEach
    public void before() {

        Configuration configuration = TestHelper.defaultConfig().build();
        connectorConfig = new VitessConnectorConfig(configuration);
        VitessTaskContext taskContext = new VitessTaskContext(TestHelper.defaultConfig().build(), connectorConfig);
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig), new CustomConverterRegistry(Collections.emptyList()), taskContext);
        decoder = new VStreamOutputMessageDecoder(schema);
    }

    @Test
    public void shouldProcessBeginEvent() throws Exception {
        // setup fixture
        String expectedShard = "shard";
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setShard(expectedShard)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
                    assertThat(message.getShard()).isEqualTo(expectedShard);
                    assertThat(message.getTransactionId()).isEqualTo(newVgtid.toString());
                    assertThat(vgtid).isEqualTo(newVgtid);
                    processed[0] = true;
                },
                newVgtid,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessHeartbeatEvent() throws Exception {
        // setup fixture
        String expectedShard = "shard";
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.HEARTBEAT)
                .setShard(expectedShard)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(HeartbeatMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.HEARTBEAT);
                    processed[0] = true;
                },
                newVgtid,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("DBZ-4667")
    public void shouldNotProcessBeginEventIfNoVgtid() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isFalse();
    }

    @Test
    public void shouldProcessCommitEvent() throws Exception {
        String expectedShard = "shard";
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(AnonymousValue.getLong())
                .setShard(expectedShard)
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);
        decoder.setTransactionId(newVgtid.toString());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
                    assertThat(message.getShard()).isEqualTo(expectedShard);
                    assertThat(message.getTransactionId()).isEqualTo(newVgtid.toString());
                    assertThat(vgtid).isEqualTo(newVgtid);
                    processed[0] = true;
                },
                newVgtid,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("DBZ-4667")
    public void shouldNotProcessCommitEventIfNoVgtid() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isFalse();
    }

    @Test
    public void shouldProcessDdlEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setTimestamp(AnonymousValue.getLong())
                .setStatement("ALTER TABLE foo ADD bar INT default 10")
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(DdlMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DDL);
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessOtherEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.OTHER)
                .setTimestamp(AnonymousValue.getLong())
                .build();

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(OtherMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.OTHER);
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessFieldEventExcludeKeyspaceFromTableName() throws Exception {
        // exercise SUT
        Configuration configuration = TestHelper.defaultConfig().with(
                VitessConnectorConfig.EXCLUDE_KEYSPACE_FROM_TABLE_NAME, true)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        VitessTaskContext taskContext = new VitessTaskContext(TestHelper.defaultConfig().build(), connectorConfig);
        VitessDatabaseSchema schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig),
                new CustomConverterRegistry(Collections.emptyList()), taskContext);
        VStreamOutputMessageDecoder decoder = new VStreamOutputMessageDecoder(schema);
        decoder.processMessage(TestHelper.defaultFieldEventExcludeKeyspaceFromTableName(),
                null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }
    }

    @Test
    public void shouldProcessFieldEvent() throws Exception {
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }
    }

    @Test
    public void shouldProcessFieldEventWithEnumSetStringsFlagDisabledAndNoCopy() throws Exception {
        // exercise SUT
        List<TestHelper.ColumnValue> columnValues = List.of(new TestHelper.ColumnValue("enum", Query.Type.ENUM, Types.VARCHAR, "foo".getBytes(), "foo"));
        Binlogdata.VEvent event = TestHelper.newFieldEvent(columnValues, TestHelper.TEST_SHARD, TestHelper.TEST_UNSHARDED_KEYSPACE, false);

        decoder.processMessage(event, null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(columnValues.size());
        List<Integer> intTypes = List.of(Types.BIGINT, Types.INTEGER);
        for (Query.Field field : event.getFieldEvent().getFieldsList()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
            String name = field.getName();
            assertThat(intTypes.contains(table.columnWithName(field.getName()).jdbcType())).isTrue();
        }
    }

    @Test
    public void shouldProcessFieldEventWithEnumSetStringsFlagEnabledAndNoCopy() throws Exception {
        // exercise SUT
        List<TestHelper.ColumnValue> columnValues = List.of(
                new TestHelper.ColumnValue("enum", Query.Type.ENUM, Types.VARCHAR, "foo".getBytes(), "foo", List.of("foo", "bar"), "enum"),
                new TestHelper.ColumnValue("set", Query.Type.SET, Types.VARCHAR, "foo".getBytes(), "foo", List.of("foo", "bar"), "set"));
        Binlogdata.VEvent event = TestHelper.newFieldEvent(columnValues, TestHelper.TEST_SHARD, TestHelper.TEST_UNSHARDED_KEYSPACE, true);

        decoder.processMessage(event, null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(columnValues.size());
        for (Query.Field field : event.getFieldEvent().getFieldsList()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
            assertThat((table.columnWithName(field.getName()).jdbcType())).isEqualTo(Types.VARCHAR);
        }
    }

    @Test
    public void shouldHandleAddColumnPerShard() throws Exception {
        String shard1 = "-80";
        String shard2 = "80-";
        // exercise SUT
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        Table table = schema.tableFor(new TableId(shard1, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
        for (Query.Field field : TestHelper.fieldsSubset()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false);

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false);

        // update schema for shard 2
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        Table tableAfterSchemaChange = schema.tableFor(new TableId(shard2, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(tableAfterSchemaChange).isNotNull();
        assertThat(tableAfterSchemaChange.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(tableAfterSchemaChange.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(tableAfterSchemaChange.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(tableAfterSchemaChange.columnWithName(field.getName())).isNotNull();
        }

        // shard 2 has been updated with new schema, so should handle values that match the new schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false);

        // shard 1 has not been updated with new schema so it should still be able to handle values with the old schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false);
    }

    @Test
    public void shouldHandleRemoveColumnPerShard() throws Exception {
        String shard1 = "-80";
        String shard2 = "80-";
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        decoder.processMessage(TestHelper.defaultFieldEvent(shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        Table table = schema.tableFor(new TableId(shard1, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false);

        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false);

        // update schema for shard 2
        decoder.processMessage(TestHelper.newFieldEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                null, null, false);
        Table tableAfterSchemaChange = schema.tableFor(new TableId(shard2, TestHelper.TEST_SHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // verify outcome
        assertThat(tableAfterSchemaChange).isNotNull();
        assertThat(tableAfterSchemaChange.id().schema()).isEqualTo(TestHelper.TEST_SHARDED_KEYSPACE);
        assertThat(tableAfterSchemaChange.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(tableAfterSchemaChange.columns().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
        for (Query.Field field : TestHelper.fieldsSubset()) {
            assertThat(tableAfterSchemaChange.columnWithName(field.getName())).isNotNull();
        }

        // shard 2 has been updated with new schema, so should handle values that match the new schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.columnValuesSubset(), shard2, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.columnSubsetNumOfColumns());
                },
                null, false);

        // shard 1 has not been updated with new schema so it should still be able to handle values with the old schema
        decoder.processMessage(
                TestHelper.insertEvent(TestHelper.defaultColumnValues(), shard1, TestHelper.TEST_SHARDED_KEYSPACE),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                },
                null, false);
    }

    @Test
    public void shouldThrowExceptionWithDetailedMessageOnRowSchemaMismatch() throws Exception {
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        Table table = schema.tableFor(TestHelper.defaultTableId());

        // verify outcome
        assertThat(table).isNotNull();
        assertThat(table.id().schema()).isEqualTo(TestHelper.TEST_UNSHARDED_KEYSPACE);
        assertThat(table.id().table()).isEqualTo(TestHelper.TEST_TABLE);
        assertThat(table.columns().size()).isEqualTo(TestHelper.defaultNumOfColumns());
        for (Query.Field field : TestHelper.defaultFields()) {
            assertThat(table.columnWithName(field.getName())).isNotNull();
        }

        assertThatThrownBy(() -> {
            decoder.processMessage(TestHelper.insertEvent(
                    TestHelper.columnValuesSubset()), null, null, false);
        }).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("bool_col")
                .hasMessageContaining("long_col");
    }

    @Test
    public void shouldProcessInsertEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultInsertEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
                    assertThat(message.getShard()).isEqualTo(TestHelper.TEST_SHARD);
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessDeleteEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultDeleteEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DELETE);
                    assertThat(message.getNewTupleList()).isNull();
                    assertThat(message.getOldTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    public void shouldProcessUpdateEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        schema.tableFor(TestHelper.defaultTableId());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultUpdateEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.UPDATE);
                    assertThat(message.getOldTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    assertThat(message.getNewTupleList().size()).isEqualTo(TestHelper.defaultNumOfColumns());
                    processed[0] = true;
                },
                null,
                false);
        assertThat(processed[0]).isTrue();
    }

    /** A table with an INT primary key, a TEXT column, a BLOB column and a VARCHAR column. */
    private static List<TestHelper.ColumnValue> noBlobColumnValues(byte[] text, byte[] blob, byte[] varchar) {
        return List.of(
                new TestHelper.ColumnValue("id", Query.Type.INT32, Types.INTEGER, "1".getBytes(), 1),
                new TestHelper.ColumnValue("text_col", Query.Type.TEXT, Types.VARCHAR, text, text == null ? null : new String(text)),
                new TestHelper.ColumnValue("blob_col", Query.Type.BLOB, Types.BLOB, blob, blob),
                new TestHelper.ColumnValue("varchar_col", Query.Type.VARCHAR, Types.VARCHAR, varchar, varchar == null ? null : new String(varchar)));
    }

    private static boolean isUnavailable(ReplicationMessage.Column column) {
        return ((ReplicationMessageColumn) column).isUnavailable();
    }

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldMarkBlobAndTextColumnsOmittedFromPartialRowImageAsUnavailable() throws Exception {
        // setup fixture: with binlog_row_image=NOBLOB the unchanged TEXT and BLOB columns are
        // omitted from the AFTER image. They arrive as NULL cells, distinguishable from a real
        // NULL only via the data_columns bitmap. varchar_col is a real NULL, present in the bitmap.
        // No before_data_columns (Vitess < 25): the AFTER bitmap is applied to the BEFORE image.
        decoder.processMessage(TestHelper.newFieldEvent(noBlobColumnValues("t".getBytes(), "b".getBytes(), "v".getBytes())), null, null, false);
        List<TestHelper.ColumnValue> before = noBlobColumnValues(null, null, "v".getBytes());
        List<TestHelper.ColumnValue> after = noBlobColumnValues(null, null, null);
        Binlogdata.RowChange.Bitmap dataColumns = TestHelper.dataColumnsBitmap(4, 0, 3);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.newUpdateEvent(before, after, dataColumns),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.UPDATE);
                    List<ReplicationMessage.Column> newColumns = message.getNewTupleList();
                    assertThat(newColumns).hasSize(4);
                    assertThat(isUnavailable(newColumns.get(0))).isFalse();
                    assertThat(isUnavailable(newColumns.get(1))).as("omitted TEXT column").isTrue();
                    assertThat(isUnavailable(newColumns.get(2))).as("omitted BLOB column").isTrue();
                    assertThat(isUnavailable(newColumns.get(3))).as("a real NULL is not unavailable").isFalse();
                    assertThat(newColumns.get(1).getValue(false, TemporalPrecisionMode.ADAPTIVE)).isSameAs(VitessValueConverter.UNAVAILABLE_VALUE);
                    assertThat(newColumns.get(3).getValue(false, TemporalPrecisionMode.ADAPTIVE)).isNull();
                    // A column omitted from the AFTER image was omitted from the BEFORE image too.
                    List<ReplicationMessage.Column> oldColumns = message.getOldTupleList();
                    assertThat(isUnavailable(oldColumns.get(1))).isTrue();
                    assertThat(isUnavailable(oldColumns.get(2))).isTrue();
                    assertThat(isUnavailable(oldColumns.get(0))).isFalse();
                    assertThat(isUnavailable(oldColumns.get(3))).isFalse();
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldUseBeforeDataColumnsBitmapForBeforeImageWhenPresent() throws Exception {
        // setup fixture: Vitess 25+ describes the BEFORE image with before_data_columns.
        // text_col changed, so it is present in the AFTER image but (like every non-PK
        // BLOB/TEXT column under NOBLOB) absent from the BEFORE image; blob_col is unchanged
        // and absent from both.
        decoder.processMessage(TestHelper.newFieldEvent(noBlobColumnValues("t".getBytes(), "b".getBytes(), "v".getBytes())), null, null, false);
        List<TestHelper.ColumnValue> before = noBlobColumnValues(null, null, "v".getBytes());
        List<TestHelper.ColumnValue> after = noBlobColumnValues("new".getBytes(), null, "v".getBytes());
        Binlogdata.RowChange.Bitmap dataColumns = TestHelper.dataColumnsBitmap(4, 0, 1, 3);
        Binlogdata.RowChange.Bitmap beforeDataColumns = TestHelper.dataColumnsBitmap(4, 0, 3);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.newUpdateEvent(before, after, dataColumns, beforeDataColumns),
                (message, vgtid) -> {
                    // verify outcome
                    List<ReplicationMessage.Column> newColumns = message.getNewTupleList();
                    assertThat(isUnavailable(newColumns.get(1))).as("changed TEXT column is present after").isFalse();
                    assertThat(newColumns.get(1).getValue(false, TemporalPrecisionMode.ADAPTIVE)).isEqualTo("new");
                    assertThat(isUnavailable(newColumns.get(2))).as("unchanged BLOB column is absent after").isTrue();
                    List<ReplicationMessage.Column> oldColumns = message.getOldTupleList();
                    assertThat(isUnavailable(oldColumns.get(1))).as("changed TEXT column is absent before").isTrue();
                    assertThat(isUnavailable(oldColumns.get(2))).as("unchanged BLOB column is absent before").isTrue();
                    assertThat(isUnavailable(oldColumns.get(0))).isFalse();
                    assertThat(isUnavailable(oldColumns.get(3))).isFalse();
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldMarkBlobAndTextColumnsOmittedFromDeleteRowImageAsUnavailable() throws Exception {
        // setup fixture: a DELETE only has a BEFORE image; with before_data_columns (Vitess 25+)
        // the omitted TEXT and BLOB columns can be told apart from NULL. Without it (older
        // Vitess) they cannot and stay NULL.
        decoder.processMessage(TestHelper.newFieldEvent(noBlobColumnValues("t".getBytes(), "b".getBytes(), "v".getBytes())), null, null, false);
        List<TestHelper.ColumnValue> row = noBlobColumnValues(null, null, "v".getBytes());

        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.newDeleteEvent(row, TestHelper.dataColumnsBitmap(4, 0, 3)),
                (message, vgtid) -> {
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.DELETE);
                    List<ReplicationMessage.Column> oldColumns = message.getOldTupleList();
                    assertThat(isUnavailable(oldColumns.get(0))).isFalse();
                    assertThat(isUnavailable(oldColumns.get(1))).isTrue();
                    assertThat(isUnavailable(oldColumns.get(2))).isTrue();
                    assertThat(isUnavailable(oldColumns.get(3))).isFalse();
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();

        processed[0] = false;
        decoder.processMessage(
                TestHelper.newDeleteEvent(row),
                (message, vgtid) -> {
                    for (ReplicationMessage.Column column : message.getOldTupleList()) {
                        assertThat(isUnavailable(column)).as(column.getName()).isFalse();
                    }
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldOnlyMarkBlobAndTextColumnsAsUnavailable() throws Exception {
        // setup fixture: a bitmap that claims a non-BLOB/TEXT column (varchar_col) is absent.
        // MySQL never omits such columns with NOBLOB, so like the MySQL connector we leave
        // the column alone (NULL) rather than substituting the placeholder.
        decoder.processMessage(TestHelper.newFieldEvent(noBlobColumnValues("t".getBytes(), "b".getBytes(), "v".getBytes())), null, null, false);
        List<TestHelper.ColumnValue> row = noBlobColumnValues("t".getBytes(), "b".getBytes(), null);
        Binlogdata.RowChange.Bitmap dataColumns = TestHelper.dataColumnsBitmap(4, 0, 1, 2);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.newUpdateEvent(row, row, dataColumns),
                (message, vgtid) -> {
                    // verify outcome
                    for (ReplicationMessage.Column column : message.getNewTupleList()) {
                        assertThat(isUnavailable(column)).as(column.getName()).isFalse();
                    }
                    assertThat(message.getNewTupleList().get(3).getValue(false, TemporalPrecisionMode.ADAPTIVE)).isNull();
                    processed[0] = true;
                },
                null, false);
        assertThat(processed[0]).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldNotMarkColumnsAsUnavailableForFullRowImages() throws Exception {
        // setup fixture: no data_columns bitmap (full row image) with real NULLs in the
        // TEXT and BLOB columns, and separately a bitmap with every column present as sent
        // for partial JSON updates with binlog_row_value_options=PARTIAL_JSON.
        decoder.processMessage(TestHelper.newFieldEvent(noBlobColumnValues("t".getBytes(), "b".getBytes(), "v".getBytes())), null, null, false);
        List<TestHelper.ColumnValue> row = noBlobColumnValues(null, null, "v".getBytes());

        for (Binlogdata.RowChange.Bitmap dataColumns : new Binlogdata.RowChange.Bitmap[]{ null, TestHelper.dataColumnsBitmap(4, 0, 1, 2, 3) }) {
            // exercise SUT
            final boolean[] processed = { false };
            decoder.processMessage(
                    TestHelper.newUpdateEvent(row, row, dataColumns),
                    (message, vgtid) -> {
                        // verify outcome
                        for (ReplicationMessage.Column column : message.getNewTupleList()) {
                            assertThat(isUnavailable(column)).as(column.getName()).isFalse();
                        }
                        for (ReplicationMessage.Column column : message.getOldTupleList()) {
                            assertThat(isUnavailable(column)).as(column.getName()).isFalse();
                        }
                        processed[0] = true;
                    },
                    null, false);
            assertThat(processed[0]).isTrue();
        }
    }

    @Test
    public void shouldSetRowEventsToCommitTimestamp() throws Exception {
        // setup fixture
        Long expectedBeginTimestamp = 1L;
        Long expectedCommitTimestamp = 2L;
        Binlogdata.VEvent beginEvent = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setTimestamp(expectedBeginTimestamp)
                .build();
        Binlogdata.VEvent commitEvent = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(expectedCommitTimestamp)
                .build();
        decoder.setCommitTimestamp(Instant.ofEpochSecond(commitEvent.getTimestamp()));
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        schema.tableFor(TestHelper.defaultTableId());
        schema.tableFor(TestHelper.defaultTableId());
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        // exercise SUT
        decoder.processMessage(
                beginEvent,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedBeginTimestamp);
                },
                newVgtid,
                false);
        decoder.processMessage(
                TestHelper.defaultInsertEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedCommitTimestamp);
                },
                null,
                false);
        decoder.processMessage(
                TestHelper.defaultUpdateEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedCommitTimestamp);
                },
                null,
                false);
        decoder.processMessage(
                TestHelper.defaultDeleteEvent(),
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedCommitTimestamp);
                },
                null,
                false);
        decoder.processMessage(
                commitEvent,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedCommitTimestamp);
                },
                newVgtid,
                false);
    }

    @Test
    public void shouldSetOtherEventsToEventTimestamp() throws Exception {
        Long expectedEventTimestamp = 1L;
        Long expectedCommitTimestamp = 2L;
        Binlogdata.VEvent otherEvent = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.OTHER)
                .setTimestamp(expectedEventTimestamp)
                .build();
        Binlogdata.VEvent ddlEvent = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.DDL)
                .setTimestamp(expectedEventTimestamp)
                .build();
        Binlogdata.VEvent commitEvent = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(expectedCommitTimestamp)
                .build();
        decoder.setCommitTimestamp(Instant.ofEpochSecond(commitEvent.getTimestamp()));
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        decoder.processMessage(
                otherEvent,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedEventTimestamp);
                },
                newVgtid,
                false);
        decoder.processMessage(
                ddlEvent,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedEventTimestamp);
                },
                null,
                false);
        decoder.processMessage(
                commitEvent,
                (message, vgtid) -> {
                    // verify outcome
                    assertThat(message.getCommitTime().getEpochSecond()).isEqualTo(expectedCommitTimestamp);
                },
                null,
                false);
    }
}
