/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.vitess.connection.ReplicationMessage;
import io.debezium.data.Envelope;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Used by {@link EventDispatcher} to get the {@link SourceRecord} {@link Struct} and pass it to a
 * {@link Receiver}, which in turn enqueue the {@link SourceRecord} to {@link ChangeEventQueue}.
 */
class VitessChangeRecordEmitter extends RelationalChangeRecordEmitter<VitessPartition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessChangeRecordEmitter.class);

    private final ReplicationMessage message;
    private final VitessDatabaseSchema schema;
    private final VitessConnectorConfig connectorConfig;
    private final Table table;
    private final boolean includeUnknownDatatypes;
    private final TemporalPrecisionMode temporalPrecisionMode;

    VitessChangeRecordEmitter(
                              VitessPartition partition,
                              VitessOffsetContext offsetContext,
                              Clock clock,
                              VitessConnectorConfig connectorConfig,
                              VitessDatabaseSchema schema,
                              ReplicationMessage message) {
        super(partition, offsetContext, clock, connectorConfig);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.table = message.getTable();
        this.includeUnknownDatatypes = connectorConfig.includeUnknownDatatypes();
        this.temporalPrecisionMode = connectorConfig.getTemporalPrecisionMode();
    }

    @Override
    public Envelope.Operation getOperation() {
        switch (message.getOperation()) {
            case INSERT:
                return Envelope.Operation.CREATE;
            case UPDATE:
                return Envelope.Operation.UPDATE;
            case DELETE:
                return Envelope.Operation.DELETE;
            default:
                throw new IllegalArgumentException(
                        "Received event of unexpected command type: " + message.getOperation());
        }
    }

    @Override
    protected Object[] getOldColumnValues() {
        switch (getOperation()) {
            case CREATE:
                return null;
            default:
                // UPDATE and DELETE have old values
                return columnValues(message.getOldTupleList());
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        switch (getOperation()) {
            case CREATE:
            case UPDATE:
                return columnValues(message.getNewTupleList());
            default:
                // DELETE does not have new values
                return null;
        }
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns) {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        Objects.requireNonNull(table);

        Object[] values = new Object[columns.size()];
        for (ReplicationMessage.Column column : columns) {
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            int position = getPosition(columnName, table, values.length);
            if (position != -1) {
                Object value = column.getValue(includeUnknownDatatypes, temporalPrecisionMode);
                values[position] = value;
            }
            else {
                LOGGER.error("Can not find position for {} in {}", columnName, table);
            }
        }
        return values;
    }

    private int getPosition(String columnName, Table table, int maxPosition) {
        final Column tableColumn = table.columnWithName(columnName);
        if (tableColumn == null) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        int position = tableColumn.position() - 1;
        if (position < 0 || position >= maxPosition) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        return position;
    }
}
