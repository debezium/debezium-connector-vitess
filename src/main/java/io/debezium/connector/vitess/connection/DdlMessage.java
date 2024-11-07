/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

/** Whether this message represents a DDL event. We don't have the DDL statement here because we don't need it.*/
public class DdlMessage implements ReplicationMessage {

    private final String transactionId;
    private final Instant commitTime;
    private final Operation operation;
    private final String statement;
    private final String shard;
    private final String keyspace;

    public DdlMessage(String transactionId, Instant commitTime, String statement, String keyspace, String shard) {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.operation = Operation.DDL;
        this.statement = statement;
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
    public String getTable() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStatement() {
        return statement;
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String getShard() {
        return shard;
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
        return false;
    }

    @Override
    public String toString() {
        return "DdlMessage{"
                + "transactionId='"
                + transactionId
                + '\''
                + ", keyspace="
                + keyspace
                + ", shard="
                + shard
                + ", commitTime="
                + commitTime
                + ", statement="
                + statement
                + ", operation="
                + operation
                + '}';
    }
}
