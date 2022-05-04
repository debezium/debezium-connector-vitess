/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.TransactionalMessage;
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.connector.vitess.connection.VStreamOutputReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.relational.TableId;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class VitessChangeRecordEmitterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordEmitterTest.class);

    private static VitessConnectorConfig connectorConfig;
    private static VitessDatabaseSchema schema;
    private static VStreamOutputMessageDecoder decoder;

    @BeforeClass
    public static void beforeClass() throws Exception {
        connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig));
        decoder = new VStreamOutputMessageDecoder(schema);
        // initialize schema by FIELD event
        decoder.processMessage(TestHelper.defaultFieldEvent(), null, null, false);
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
                TestHelper.defaultRelationMessageColumns());

        // exercise SUT
        VitessChangeRecordEmitter emitter = new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);

        // verify outcome
        assertThat(emitter.getOperation()).isEqualTo(Envelope.Operation.CREATE);
        assertThat(emitter.getOldColumnValues()).isNull();
        assertThat(emitter.getNewColumnValues()).isEqualTo(TestHelper.defaultJavaValues().toArray());
    }

    @Test
    public void shouldGetOldColumnValuesFromDelete() {
        // setup fixture
        ReplicationMessage message = new VStreamOutputReplicationMessage(
                ReplicationMessage.Operation.DELETE,
                AnonymousValue.getInstant(),
                AnonymousValue.getString(),
                new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE)
                        .toDoubleQuotedString(),
                TestHelper.defaultRelationMessageColumns(),
                null);

        // exercise SUT
        VitessChangeRecordEmitter emitter = new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);

        // verify outcome
        assertThat(emitter.getOperation()).isEqualTo(Envelope.Operation.DELETE);
        assertThat(emitter.getOldColumnValues()).isEqualTo(TestHelper.defaultJavaValues().toArray());
        assertThat(emitter.getNewColumnValues()).isNull();
    }

    @Test
    public void shouldGetOldAndNewColumnValuesFromUpdate() {
        // setup fixture
        ReplicationMessage message = new VStreamOutputReplicationMessage(
                ReplicationMessage.Operation.UPDATE,
                AnonymousValue.getInstant(),
                AnonymousValue.getString(),
                new TableId(null, TestHelper.TEST_UNSHARDED_KEYSPACE, TestHelper.TEST_TABLE)
                        .toDoubleQuotedString(),
                TestHelper.defaultRelationMessageColumns(),
                TestHelper.defaultRelationMessageColumns());

        // exercise SUT
        VitessChangeRecordEmitter emitter = new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);

        // verify outcome
        assertThat(emitter.getOperation()).isEqualTo(Envelope.Operation.UPDATE);
        assertThat(emitter.getOldColumnValues()).isEqualTo(TestHelper.defaultJavaValues().toArray());
        assertThat(emitter.getNewColumnValues()).isEqualTo(TestHelper.defaultJavaValues().toArray());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportBeginMessage() {
        // setup fixture
        ReplicationMessage message = new TransactionalMessage(ReplicationMessage.Operation.BEGIN, AnonymousValue.getString(), AnonymousValue.getInstant());

        // exercise SUT
        new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldNotSupportCommitMessage() {
        // setup fixture
        ReplicationMessage message = new TransactionalMessage(ReplicationMessage.Operation.COMMIT, AnonymousValue.getString(), AnonymousValue.getInstant());

        // exercise SUT
        new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);
    }

    private VitessPartition initializePartition() {
        return new VitessPartition("test");
    }
}
