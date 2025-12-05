/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.LoggingContext;

public class VitessHeartbeatImplTest {

    private final ChangeEventQueue<DataChangeEvent> eventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
            .maxBatchSize(10)
            .maxQueueSize(20)
            .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
            .pollInterval(Duration.ofMillis(500))
            .queueProvider(new DefaultQueueProvider<>(20))
            .build();

    private final VitessHeartbeatImpl underTest = new VitessHeartbeatImpl(Duration.ofMillis(1), "topicName", "key", SchemaNameAdjuster.NO_OP, eventQueue);

    @Test
    public void shouldNotSendRecordIfNoOffset() throws InterruptedException {
        underTest.emit(null, null);

        assertThat(eventQueue.poll()).isEmpty();
    }

    @Test
    public void shouldSendRecordIfOffsetPresent() throws InterruptedException {
        underTest.emit(null, createOffsetContext(Map.of("foo", "bar")));

        assertThat(eventQueue.poll()).isNotEmpty();
    }

    @Test
    public void shouldSendRecordWithVgtid() throws InterruptedException {
        ChangeEventQueue<DataChangeEvent> eventQueue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(10)
                .maxQueueSize(20)
                .loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c"))
                .pollInterval(Duration.ofMillis(500))
                .queueProvider(new DefaultQueueProvider<>(20))
                .build();

        Heartbeat heartbeat = new VitessHeartbeatImpl(Duration.ofMillis(1), "topicName", "key", SchemaNameAdjuster.NO_OP, eventQueue);

        heartbeat.emit(null, createOffsetContext(Map.of("vgtid", "bar")));

        DataChangeEvent actual = eventQueue.poll().get(0);

        assertThat(actual).isNotNull();
        Struct value = (Struct) actual.getRecord().value();
        assertThat(value.get(SourceInfo.VGTID_KEY)).isEqualTo("bar");
    }

    private static OffsetContext createOffsetContext(final Map<String, String> offset) {
        return new OffsetContext() {
            @Override
            public Map<String, ?> getOffset() {
                return offset;
            }

            @Override
            public Schema getSourceInfoSchema() {
                return null;
            }

            @Override
            public Struct getSourceInfo() {
                return null;
            }

            @Override
            public boolean isInitialSnapshotRunning() {
                return false;
            }

            @Override
            public void markSnapshotRecord(SnapshotRecord record) {

            }

            @Override
            public void preSnapshotStart(boolean onDemand) {

            }

            @Override
            public void preSnapshotCompletion() {

            }

            @Override
            public void postSnapshotCompletion() {

            }

            @Override
            public void event(DataCollectionId collectionId, Instant timestamp) {

            }

            @Override
            public TransactionContext getTransactionContext() {
                return null;
            }
        };
    }

}
