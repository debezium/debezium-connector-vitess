/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.time.Instant;

import org.junit.Test;

public class HeartbeatMessageTest {

    @Test
    public void getOperation() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThat(message.getOperation()).isEqualTo(ReplicationMessage.Operation.HEARTBEAT);
    }

    @Test
    public void getCommitTime() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThat(message.getCommitTime()).isEqualTo(Instant.EPOCH);
    }

    @Test
    public void getTransactionId() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThatThrownBy(() -> message.getTransactionId())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getTable() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThatThrownBy(() -> message.getTableName())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getShard() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThatThrownBy(() -> message.getShard())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getOldTupleList() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThatThrownBy(() -> message.getOldTupleList())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void getNewTupleList() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThatThrownBy(() -> message.getNewTupleList())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testToString() {
        ReplicationMessage message = new HeartbeatMessage(Instant.EPOCH);
        assertThat(message.toString()).isEqualTo("HeartbeatMessage{commitTime=1970-01-01T00:00:00Z, operation=HEARTBEAT}");
    }
}
