/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessHeartbeatImplTest {

    class CapturingBlockingConsumer implements BlockingConsumer<SourceRecord> {

        private final List<SourceRecord> records = new ArrayList<>();

        @Override
        public void accept(SourceRecord record) throws InterruptedException {
            this.records.add(record);
        }

        public List<SourceRecord> getRecords() {
            return records;
        }
    }

    @Test
    public void shouldNotSendRecordIfNoOffset() throws InterruptedException {
        Heartbeat heartbeat = new VitessHeartbeatImpl(Duration.ofMillis(1), "topicName", "key", SchemaNameAdjuster.NO_OP);
        CapturingBlockingConsumer blockingConsumer = new CapturingBlockingConsumer();
        heartbeat.forcedBeat(null, null, blockingConsumer);
        assertThat(blockingConsumer.getRecords()).isEmpty();
    }

    @Test
    public void shouldSendRecordIfOffsetPresent() throws InterruptedException {
        Heartbeat heartbeat = new VitessHeartbeatImpl(Duration.ofMillis(1), "topicName", "key", SchemaNameAdjuster.NO_OP);
        CapturingBlockingConsumer blockingConsumer = new CapturingBlockingConsumer();
        Map<String, ?> offset = Map.of("foo", "bar");
        heartbeat.forcedBeat(null, offset, blockingConsumer);
        assertThat(blockingConsumer.getRecords()).isNotEmpty();
    }

    @Test
    public void shouldSendRecordWithVgtid() throws InterruptedException {
        Heartbeat heartbeat = new VitessHeartbeatImpl(Duration.ofMillis(1), "topicName", "key", SchemaNameAdjuster.NO_OP);
        CapturingBlockingConsumer blockingConsumer = new CapturingBlockingConsumer();
        String expectedVgtid = "bar";
        Map<String, ?> offset = Map.of("vgtid", expectedVgtid);
        heartbeat.forcedBeat(null, offset, blockingConsumer);
        SourceRecord record = blockingConsumer.getRecords().get(0);
        assertThat(record).isNotNull();
        Struct value = (Struct) record.value();
        assertThat(value.get(SourceInfo.VGTID_KEY)).isEqualTo(expectedVgtid);
    }

}
