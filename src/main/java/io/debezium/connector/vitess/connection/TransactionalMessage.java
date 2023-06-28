/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

/** Whether this message represents the begin or end of a transaction. */
public class TransactionalMessage implements ReplicationMessage {

    private final String transactionId;
    private final Instant commitTime;
    private final Operation operation;

    public TransactionalMessage(Operation operation, String transactionId, Instant commitTime) {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.operation = operation;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public Instant getCommitTime() {
        return commitTime;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
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
    public boolean isTransactionalMessage() {
        return true;
    }

    @Override
    public String toString() {
        return "TransactionalMessage{"
                + "transactionId='"
                + transactionId
                + '\''
                + ", commitTime="
                + commitTime
                + ", operation="
                + operation
                + '}';
    }
}
