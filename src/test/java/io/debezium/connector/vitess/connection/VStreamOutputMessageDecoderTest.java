/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.AnonymousValue;
import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.doc.FixFor;
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

    @Before
    public void before() {
        connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(schema);
    }

    @Test
    public void shouldProcessBeginEvent() throws Exception {
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.BEGIN)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.BEGIN);
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
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
        // setup fixture
        Binlogdata.VEvent event = Binlogdata.VEvent.newBuilder()
                .setType(Binlogdata.VEventType.COMMIT)
                .setTimestamp(AnonymousValue.getLong())
                .build();
        Vgtid newVgtid = Vgtid.of(VgtidTest.VGTID_JSON);
        decoder.setTransactionId(newVgtid.toString());

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                event,
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(TransactionalMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.COMMIT);
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
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
    public void shouldProcessFieldEvent() throws Exception {
        // exercise SUT
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        Table table = schema.tableFor(new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE));

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
    public void shouldProcessInsertEvent() throws Exception {
        // setup fixture
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
        Table table = schema.tableFor(new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultInsertEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
                    // verify outcome
                    assertThat(message).isNotNull();
                    assertThat(message).isInstanceOf(VStreamOutputReplicationMessage.class);
                    assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.INSERT);
                    assertThat(message.getOldTupleList()).isNull();
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
        Table table = schema.tableFor(new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultDeleteEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
        Table table = schema.tableFor(new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE));

        // exercise SUT
        final boolean[] processed = { false };
        decoder.processMessage(
                TestHelper.defaultUpdateEvent(),
                (message, vgtid, isLastRowEventOfTransaction) -> {
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
}
