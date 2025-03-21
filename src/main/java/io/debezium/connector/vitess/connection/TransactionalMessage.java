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
    private final String keyspace;
    private final String shard;

    public TransactionalMessage(Operation operation, String transactionId, Instant commitTime, String keyspace, String shard) {
        if (operation != Operation.BEGIN && operation != Operation.COMMIT) {
            throw new IllegalArgumentException("TransactionalMessage can only have BEGIN or COMMIT operations");
        }
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.operation = operation;
        this.keyspace = keyspace;
        this.shard = shard;
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
    public String getKeyspace() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getShard() {
        return shard;
    }

    @Override
    public String getStatement() {
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
