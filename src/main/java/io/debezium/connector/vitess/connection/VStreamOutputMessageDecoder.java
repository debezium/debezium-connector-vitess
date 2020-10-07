/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import static io.debezium.connector.vitess.connection.ReplicationMessage.Column;

import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.VitessType;
import io.debezium.connector.vitess.connection.ReplicationMessage.Operation;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.Row;

import binlogdata.Binlogdata;

public class VStreamOutputMessageDecoder implements MessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VStreamOutputMessageDecoder.class);

    private Instant commitTimestamp;
    private String transactionId = null;

    private final VitessDatabaseSchema schema;

    public VStreamOutputMessageDecoder(VitessDatabaseSchema schema) {
        this.schema = schema;
    }

    @Override
    public void processMessage(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        final Binlogdata.VEventType vEventType = vEvent.getType();
        switch (vEventType) {
            case BEGIN:
                handleBeginMessage(vEvent, processor, newVgtid);
                break;
            case COMMIT:
                handleCommitMessage(vEvent, processor, newVgtid);
                break;
            case ROW:
                decodeRows(vEvent, processor, newVgtid);
                break;
            case FIELD:
                // field type event has table schema
                handleFieldMessage(vEvent);
                break;
            case DDL:
                handleDdl(vEvent, processor, newVgtid);
                break;
            case OTHER:
                handleOther(vEvent, processor, newVgtid);
                break;
            case VGTID:
            case VERSION:
                break;
            default:
                LOGGER.warn("vEventType {} skipped, not proccess.", vEventType);
        }
    }

    private void handleDdl(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        this.commitTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        processor.process(
                new DdlMessage(transactionId, commitTimestamp), newVgtid);
    }

    private void handleOther(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        this.commitTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        processor.process(
                new OtherMessage(transactionId, commitTimestamp), newVgtid);
    }

    private void handleBeginMessage(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        this.commitTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        LOGGER.trace("Commit timestamp of begin transaction: {}", commitTimestamp);
        processor.process(
                new TransactionalMessage(Operation.BEGIN, transactionId, commitTimestamp), newVgtid);
    }

    private void handleCommitMessage(
                                     Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Instant commitTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        LOGGER.trace("Commit timestamp of commit transaction: {}", commitTimestamp);
        processor.process(
                new TransactionalMessage(Operation.COMMIT, transactionId, commitTimestamp), newVgtid);
    }

    private void decodeRows(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Binlogdata.RowEvent rowEvent = vEvent.getRowEvent();
        String[] schemaTableTuple = rowEvent.getTableName().split("\\.");
        if (schemaTableTuple.length != 2) {
            LOGGER.error(
                    "Handling ROW vEvent. schemaTableTuple should have schema name and table name but has size {}. {} is skipped.",
                    schemaTableTuple.length,
                    rowEvent);
        }
        else {
            String schemaName = schemaTableTuple[0];
            String tableName = schemaTableTuple[1];
            for (Binlogdata.RowChange rowChange : rowEvent.getRowChangesList()) {
                if (rowChange.hasAfter() && !rowChange.hasBefore()) {
                    decodeInsert(rowChange.getAfter(), schemaName, tableName, processor, newVgtid);
                }
                else if (rowChange.hasAfter() && rowChange.hasBefore()) {
                    decodeUpdate(
                            rowChange.getBefore(), rowChange.getAfter(), schemaName, tableName, processor, newVgtid);
                }
                else if (!rowChange.hasAfter() && rowChange.hasBefore()) {
                    decodeDelete(rowChange.getBefore(), schemaName, tableName, processor, newVgtid);
                }
                else {
                    LOGGER.error("{} decodeRow skipped.", vEvent);
                }
            }
        }
    }

    private void decodeInsert(
                              Row row,
                              String schemaName,
                              String tableName,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(schemaName, tableName);

        TableId tableId;
        List<Column> columns = null;
        if (!resolvedTable.isPresent()) {
            LOGGER.debug("Row insert for {}.{} is filtered out", schemaName, tableName);
            tableId = new TableId(null, schemaName, tableName);
            // no need for columns because the event will be filtered out
        }
        else {
            Table table = resolvedTable.get();
            tableId = table.id();
            columns = resolveColumns(row, table);
        }

        processor.process(
                new VStreamOutputReplicationMessage(
                        Operation.INSERT,
                        commitTimestamp,
                        transactionId,
                        tableId.toDoubleQuotedString(),
                        null,
                        columns),
                newVgtid);
    }

    private void decodeUpdate(
                              Row oldRow,
                              Row newRow,
                              String schemaName,
                              String tableName,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(schemaName, tableName);

        TableId tableId;
        List<Column> oldColumns = null;
        List<Column> newColumns = null;
        if (!resolvedTable.isPresent()) {
            LOGGER.debug("Row update for {}.{} is filtered out", schemaName, tableName);
            tableId = new TableId(null, schemaName, tableName);
            // no need for oldColumns and newColumns because the event will be filtered out
        }
        else {
            Table table = resolvedTable.get();
            tableId = table.id();
            oldColumns = resolveColumns(oldRow, table);
            newColumns = resolveColumns(newRow, table);
        }

        processor.process(
                new VStreamOutputReplicationMessage(
                        Operation.UPDATE,
                        commitTimestamp,
                        transactionId,
                        tableId.toDoubleQuotedString(),
                        oldColumns,
                        newColumns),
                newVgtid);
    }

    private void decodeDelete(
                              Row row,
                              String schemaName,
                              String tableName,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(schemaName, tableName);

        TableId tableId;
        List<Column> columns = null;

        if (!resolvedTable.isPresent()) {
            LOGGER.debug("Row delete for {}.{} is filtered out", schemaName, tableName);
            tableId = new TableId(null, schemaName, tableName);
            // no need for columns because the event will be filtered out
        }
        else {
            Table table = resolvedTable.get();
            tableId = table.id();
            columns = resolveColumns(row, table);
        }

        processor.process(
                new VStreamOutputReplicationMessage(
                        Operation.DELETE,
                        commitTimestamp,
                        transactionId,
                        tableId.toDoubleQuotedString(),
                        columns,
                        null),
                newVgtid);
    }

    /** Resolve table from a prior FIELD message or empty when the table is filtered */
    private Optional<Table> resolveRelation(String schemaName, String tableName) {
        return Optional.ofNullable(schema.tableFor(new TableId(null, schemaName, tableName)));
    }

    /** Resolve the vEvent data to a list of replication message columns (with values). */
    private List<Column> resolveColumns(Row row, Table table) {
        int numberOfColumns = row.getLengthsCount();
        String rawValues = row.getValues().toStringUtf8();

        int rawValueIndex = 0;
        List<Column> columns = new ArrayList<>(numberOfColumns);
        for (short i = 0; i < numberOfColumns; i++) {
            final io.debezium.relational.Column column = table.columns().get(i);
            final String columnName = column.name();
            final VitessType vitessType = new VitessType(columnName, column.jdbcType());
            final boolean optional = column.isOptional();

            final int rawValueLength = (int) row.getLengths(i);
            final String rawValue = rawValueLength == -1
                    ? null
                    : rawValues.substring(rawValueIndex, rawValueIndex + rawValueLength);
            if (rawValueLength != -1) {
                // no update to rawValueIndex when no value in the rawValue
                rawValueIndex += rawValueLength;
            }
            columns.add(new ReplicationMessageColumn(columnName, vitessType, optional, rawValue));
        }
        return columns;
    }

    private void handleFieldMessage(Binlogdata.VEvent vEvent) {
        Binlogdata.FieldEvent fieldEvent = vEvent.getFieldEvent();
        if (fieldEvent == null) {
            LOGGER.error("fieldEvent is expected from {}", vEvent);
        }
        else {
            String[] schemaTableTuple = fieldEvent.getTableName().split("\\.");
            if (schemaTableTuple.length != 2) {
                LOGGER.error(
                        "Handling FIELD vEvent. schemaTableTuple should have schema name and table name but has size {}. {} is skipped",
                        schemaTableTuple.length,
                        vEvent);
            }
            else {
                String schemaName = schemaTableTuple[0];
                String tableName = schemaTableTuple[1];
                int columnCount = fieldEvent.getFieldsCount();

                LOGGER.trace("Event: {}, Columns: {}", Binlogdata.VEventType.FIELD, columnCount);
                LOGGER.trace("Schema: '{}', Table: '{}'", schemaName, tableName);

                List<ColumnMetaData> columns = new ArrayList<>(columnCount);
                for (short i = 0; i < columnCount; ++i) {
                    Field field = fieldEvent.getFields(i);
                    String columnName = field.getName();
                    String columnType = field.getType().name();
                    VitessType vitessType = VitessType.resolve(columnType);
                    if (vitessType.getJdbcId() == Types.OTHER) {
                        LOGGER.error("Cannot resolve JDBC type from vstream type {}", columnType);
                    }
                    // TODO: Vitess community has recently added pk flag
                    boolean key = false;
                    boolean optional = true;

                    columns.add(new ColumnMetaData(columnName, vitessType, key, optional));
                }

                Table table = resolveTable(schemaName, tableName, columns);
                schema.applySchemaChangesForTable(table);
            }
        }
    }

    private Table resolveTable(String schemaName, String tableName, List<ColumnMetaData> columns) {
        List<io.debezium.relational.Column> cols = new ArrayList<>(columns.size());
        for (ColumnMetaData columnMetaData : columns) {
            ColumnEditor editor = io.debezium.relational.Column.editor()
                    .name(columnMetaData.getColumnName())
                    .type(columnMetaData.getVitessType().getName())
                    .jdbcType(columnMetaData.getVitessType().getJdbcId());
            cols.add(editor.create());
        }
        Table table = Table.editor().addColumns(cols).tableId(new TableId(null, schemaName, tableName)).create();
        return table;
    }
}
