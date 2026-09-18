/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.connector.vitess.connection.ReplicationMessageColumn;
import io.debezium.connector.vitess.connection.TransactionalMessage;
import io.debezium.connector.vitess.connection.VStreamOutputMessageDecoder;
import io.debezium.connector.vitess.connection.VStreamOutputReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.doc.FixFor;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.vitess.proto.Query;

public class VitessChangeRecordEmitterTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordEmitterTest.class);

    private static VitessConnectorConfig connectorConfig;
    private static VitessDatabaseSchema schema;
    private static VStreamOutputMessageDecoder decoder;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Configuration configuration = TestHelper.defaultConfig().build();
        connectorConfig = new VitessConnectorConfig(configuration);
        VitessTaskContext taskContext = new VitessTaskContext(TestHelper.defaultConfig().build(), connectorConfig);
        schema = new VitessDatabaseSchema(
                connectorConfig,
                SchemaNameAdjuster.create(),
                (TopicNamingStrategy) DefaultTopicNamingStrategy.create(connectorConfig), new CustomConverterRegistry(Collections.emptyList()), taskContext);
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
                AnonymousValue.getString(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                AnonymousValue.getString(),
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
                AnonymousValue.getString(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                AnonymousValue.getString(),
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
                AnonymousValue.getString(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                AnonymousValue.getString(),
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

    @Test
    @FixFor("debezium/dbz#2607")
    public void shouldPassUnavailableValueSentinelThroughForOmittedColumns() {
        // setup fixture: string_col was omitted from the row image (binlog_row_image=NOBLOB);
        // the emitter hands the sentinel to the value converters, which substitute the placeholder
        List<ReplicationMessage.Column> columns = new ArrayList<>(TestHelper.defaultRelationMessageColumns());
        columns.set(3, new ReplicationMessageColumn("string_col", new VitessType(Query.Type.VARBINARY.name(), Types.VARCHAR), true, null, true));
        ReplicationMessage message = new VStreamOutputReplicationMessage(
                ReplicationMessage.Operation.UPDATE,
                AnonymousValue.getInstant(),
                AnonymousValue.getString(),
                AnonymousValue.getString(),
                TestHelper.defaultTableId().toDoubleQuotedString(),
                AnonymousValue.getString(),
                columns,
                columns);

        // exercise SUT
        VitessChangeRecordEmitter emitter = new VitessChangeRecordEmitter(
                initializePartition(),
                null,
                Clock.system(),
                new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                schema,
                message);

        // verify outcome
        List<Object> expected = new ArrayList<>(TestHelper.defaultJavaValues());
        expected.set(3, VitessValueConverter.UNAVAILABLE_VALUE);
        assertThat(emitter.getNewColumnValues()).isEqualTo(expected.toArray());
        assertThat(emitter.getOldColumnValues()).isEqualTo(expected.toArray());
    }

    @Test
    public void shouldNotSupportBeginMessage() {
        // setup fixture
        ReplicationMessage message = new TransactionalMessage(ReplicationMessage.Operation.BEGIN, AnonymousValue.getString(), AnonymousValue.getInstant(),
                AnonymousValue.getString(), AnonymousValue.getString());

        // exercise SUT
        assertThrows(UnsupportedOperationException.class, () -> {
            new VitessChangeRecordEmitter(
                    initializePartition(),
                    null,
                    Clock.system(),
                    new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                    schema,
                    message);
        });
    }

    @Test
    public void shouldNotSupportCommitMessage() {
        // setup fixture
        ReplicationMessage message = new TransactionalMessage(ReplicationMessage.Operation.COMMIT, AnonymousValue.getString(), AnonymousValue.getInstant(),
                AnonymousValue.getString(), AnonymousValue.getString());

        // exercise SUT
        assertThrows(UnsupportedOperationException.class, () -> {
            new VitessChangeRecordEmitter(
                    initializePartition(),
                    null,
                    Clock.system(),
                    new VitessConnectorConfig(TestHelper.defaultConfig().build()),
                    schema,
                    message);
        });
    }

    private VitessPartition initializePartition() {
        return new VitessPartition("test", null);
    }
}
