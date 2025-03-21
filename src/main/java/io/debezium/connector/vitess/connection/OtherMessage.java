/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

/** Whether this message represents an OTHER event.*/
public class OtherMessage implements ReplicationMessage {

    private final String transactionId;
    private final Instant commitTime;
    private final Operation operation;

    public OtherMessage(String transactionId, Instant commitTime) {
        this.transactionId = transactionId;
        this.commitTime = commitTime;
        this.operation = Operation.OTHER;
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
    public String getStatement() {
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
        return false;
    }

    @Override
    public String toString() {
        return "OtherMessage{"
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
