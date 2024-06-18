/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

public class HeartbeatMessage implements ReplicationMessage {

    private final Instant commitTime;
    private final Operation operation;

    public HeartbeatMessage(Instant commitTime) {
        this.commitTime = commitTime;
        this.operation = Operation.HEARTBEAT;
    }

    @Override
    public Operation getOperation() {
        return Operation.HEARTBEAT;
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public String getTransactionId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getShard() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getOldTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Column> getNewTupleList() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "HeartbeatMessage{"
                + "commitTime="
                + commitTime
                + ", operation="
                + operation
                + '}';
    }
}
