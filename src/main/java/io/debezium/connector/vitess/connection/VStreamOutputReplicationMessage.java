/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

/**
 * A logical representation of row-insert, row-update, row-delete. It contains each columns' type
 * and value.
 */
public class VStreamOutputReplicationMessage implements ReplicationMessage {

    private final Operation op;
    private final Instant commitTimestamp;
    private final String transactionId;
    private final String table;
    private final String shard;
    private final List<Column> oldColumns;
    private final List<Column> newColumns;

    public VStreamOutputReplicationMessage(
                                           Operation op,
                                           Instant commitTimestamp,
                                           String transactionId,
                                           String table,
                                           String shard,
                                           List<Column> oldColumns,
                                           List<Column> newColumns) {
        this.op = op;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.shard = shard;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public String getTransactionId() {
        return transactionId;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getShard() {
        return shard;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public String toString() {
        return "VStreamOutputReplicationMessage{"
                + "op="
                + op
                + ", commitTimestamp="
                + commitTimestamp
                + ", transactionId='"
                + transactionId
                + '\''
                + ", table='"
                + table
                + '\''
                + ", oldColumns="
                + oldColumns
                + ", newColumns="
                + newColumns
                + '}';
    }
}
