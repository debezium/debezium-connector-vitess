/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.time.Instant;
import java.util.List;

import io.debezium.connector.vitess.VitessType;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Logic representation of a replication message. It can be a transactional message (begin, commit),
 * or a data message (row-insert, row-delete, row-update).
 */
public interface ReplicationMessage {

    /** Data modification operation */
    enum Operation {
        INSERT,
        UPDATE,
        DELETE,
        BEGIN,
        COMMIT,
        DDL,
        OTHER,
        HEARTBEAT
    }

    /** A representation of column value delivered as a part of replication message */
    interface Column {
        String getName();

        VitessType getType();

        /**
         * Converts the value (string representation) coming from VStream to a Java value based on the
         * type of the column from the message.
         */
        Object getValue(boolean includeUnknownDatatypes, TemporalPrecisionMode temporalPrecisionMode);

        boolean isOptional();
    }

    /** Convenient wrapper that converts the raw string column value to a Java type */
    interface ColumnValue<T> {
        T getRawValue();

        boolean isNull();

        byte[] asBytes();

        String asString();

        Integer asInteger();

        Short asShort();

        Long asLong();

        Float asFloat();

        Double asDouble();

        Object asDefault(VitessType vitessType, boolean includeUnknownDatatypes);
    }

    Operation getOperation();

    Instant getCommitTime();

    String getTransactionId();

    String getKeyspace();

    String getTableName();

    default TableId getTableId() {
        throw new UnsupportedOperationException();
    }

    default Table getTable() {
        throw new UnsupportedOperationException();
    }

    String getShard();

    List<Column> getOldTupleList();

    List<Column> getNewTupleList();

    String getStatement();

    default boolean isTransactionalMessage() {
        return getOperation() == Operation.BEGIN || getOperation() == Operation.COMMIT;
    }
}
