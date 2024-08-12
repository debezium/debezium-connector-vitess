/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.connection;

import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.connector.vitess.VitessType;
import io.debezium.connector.vitess.connection.ReplicationMessage.Column;
import io.debezium.connector.vitess.connection.ReplicationMessage.Operation;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.Row;

import binlogdata.Binlogdata;

public class VStreamOutputMessageDecoder implements MessageDecoder {
    private static final Logger LOGGER = LoggerFactory.getLogger(VStreamOutputMessageDecoder.class);

    // See all flags: https://dev.mysql.com/doc/dev/mysql-server/8.0.12/group__group__cs__column__definition__flags.html
    private static final int NOT_NULL_FLAG = 1;
    private static final int PRI_KEY_FLAG = 1 << 1;
    private static final int UNIQUE_KEY_FLAG = 1 << 2;

    private Instant commitTimestamp;
    private String transactionId = null;

    private final VitessDatabaseSchema schema;

    public VStreamOutputMessageDecoder(VitessDatabaseSchema schema) {
        this.schema = schema;
    }

    public void setCommitTimestamp(Instant commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    @Override
    public void processMessage(Binlogdata.VEvent vEvent,
                               ReplicationMessageProcessor processor,
                               Vgtid newVgtid,
                               boolean isLastRowEventOfTransaction,
                               boolean isInVStreamCopy)
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
                decodeRows(vEvent, processor, newVgtid, isLastRowEventOfTransaction);
                break;
            case FIELD:
                // field type event has table schema
                handleFieldMessage(vEvent, isInVStreamCopy);
                break;
            case DDL:
                handleDdl(vEvent, processor, newVgtid);
                break;
            case OTHER:
                handleOther(vEvent, processor, newVgtid);
                break;
            case HEARTBEAT:
                handleHeartbeat(vEvent, processor, newVgtid);
            case VGTID:
            case VERSION:
                break;
            default:
                LOGGER.info("VEventType {} skipped, not processing.", vEventType);
        }
    }

    private void handleDdl(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Instant eventTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        processor.process(
                new DdlMessage(transactionId, eventTimestamp), newVgtid, false);
    }

    private void handleOther(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Instant eventTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        processor.process(
                new OtherMessage(transactionId, eventTimestamp), newVgtid, false);
    }

    private void handleHeartbeat(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid) throws InterruptedException {
        Instant eventTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        processor.process(new HeartbeatMessage(eventTimestamp), newVgtid, false);
    }

    private void handleBeginMessage(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Instant eventTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Use the entire VGTID as transaction id.
        if (newVgtid != null) {
            this.transactionId = newVgtid.toString();
        }
        // Transaction ID must not be null in TransactionalMessage.
        if (this.transactionId == null) {
            LOGGER.info("Skip processing BEGIN because no VGTID was received");
            return;
        }
        LOGGER.trace("Timestamp of begin transaction: {}", eventTimestamp);
        processor.process(
                new TransactionalMessage(Operation.BEGIN, transactionId, eventTimestamp, vEvent.getShard()), newVgtid, false);
    }

    private void handleCommitMessage(
                                     Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid)
            throws InterruptedException {
        Instant eventTimestamp = Instant.ofEpochSecond(vEvent.getTimestamp());
        // Transaction ID must not be null in TransactionalMessage.
        if (this.transactionId == null) {
            LOGGER.info("Skip processing COMMIT because no VGTID was received");
            return;
        }
        LOGGER.trace("Timestamp of commit transaction: {}", commitTimestamp);
        processor.process(
                new TransactionalMessage(Operation.COMMIT, transactionId, eventTimestamp, vEvent.getShard()), newVgtid, false);
    }

    private void decodeRows(Binlogdata.VEvent vEvent, ReplicationMessageProcessor processor, Vgtid newVgtid, boolean isLastRowEventOfTransaction)
            throws InterruptedException {
        Binlogdata.RowEvent rowEvent = vEvent.getRowEvent();
        String[] schemaTableTuple = rowEvent.getTableName().split("\\.");
        if (schemaTableTuple.length != 2) {
            LOGGER.error(
                    "Handling ROW VEvent. schemaTableTuple should have schema name and table name but has size {}. {} is skipped.",
                    schemaTableTuple.length,
                    rowEvent);
        }
        else {
            String schemaName = schemaTableTuple[0];
            String tableName = schemaTableTuple[1];
            String shard = rowEvent.getShard();
            int numOfRowChanges = rowEvent.getRowChangesCount();
            int numOfRowChangesEventSeen = 0;
            for (int i = 0; i < numOfRowChanges; i++) {
                Binlogdata.RowChange rowChange = rowEvent.getRowChanges(i);
                numOfRowChangesEventSeen++;
                boolean isLastRowOfTransaction = isLastRowEventOfTransaction && numOfRowChangesEventSeen == numOfRowChanges ? true : false;
                if (rowChange.hasAfter() && !rowChange.hasBefore()) {
                    decodeInsert(rowChange.getAfter(), schemaName, tableName, shard, processor, newVgtid, isLastRowOfTransaction);
                }
                else if (rowChange.hasAfter() && rowChange.hasBefore()) {
                    decodeUpdate(
                            rowChange.getBefore(), rowChange.getAfter(), schemaName, tableName, shard, processor, newVgtid, isLastRowOfTransaction);
                }
                else if (!rowChange.hasAfter() && rowChange.hasBefore()) {
                    decodeDelete(rowChange.getBefore(), schemaName, tableName, shard, processor, newVgtid, isLastRowOfTransaction);
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
                              String shard,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid,
                              boolean isLastRowEventOfTransaction)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(shard, schemaName, tableName);

        TableId tableId;
        List<Column> columns = null;
        if (!resolvedTable.isPresent()) {
            LOGGER.trace("Row insert for {}.{} is filtered out", schemaName, tableName);
            tableId = VitessDatabaseSchema.buildTableId(shard, schemaName, tableName);
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
                        shard,
                        null,
                        columns),
                newVgtid,
                isLastRowEventOfTransaction);
    }

    private void decodeUpdate(
                              Row oldRow,
                              Row newRow,
                              String schemaName,
                              String tableName,
                              String shard,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid,
                              boolean isLastRowEventOfTransaction)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(shard, schemaName, tableName);

        TableId tableId;
        List<Column> oldColumns = null;
        List<Column> newColumns = null;
        if (!resolvedTable.isPresent()) {
            LOGGER.trace("Row update for {}.{} is filtered out", schemaName, tableName);
            tableId = VitessDatabaseSchema.buildTableId(shard, schemaName, tableName);
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
                        shard,
                        oldColumns,
                        newColumns),
                newVgtid,
                isLastRowEventOfTransaction);
    }

    private void decodeDelete(
                              Row row,
                              String schemaName,
                              String tableName,
                              String shard,
                              ReplicationMessageProcessor processor,
                              Vgtid newVgtid,
                              boolean isLastRowOfTransaction)
            throws InterruptedException {
        Optional<Table> resolvedTable = resolveRelation(shard, schemaName, tableName);

        TableId tableId;
        List<Column> columns = null;

        if (!resolvedTable.isPresent()) {
            LOGGER.trace("Row delete for {}.{} is filtered out", schemaName, tableName);
            tableId = VitessDatabaseSchema.buildTableId(shard, schemaName, tableName);
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
                        shard,
                        columns,
                        null),
                newVgtid,
                isLastRowOfTransaction);
    }

    /** Resolve table from a prior FIELD message or empty when the table is filtered */
    private Optional<Table> resolveRelation(String shard, String schemaName, String tableName) {
        return Optional.ofNullable(schema.tableFor(VitessDatabaseSchema.buildTableId(shard, schemaName, tableName)));
    }

    /** Resolve the vEvent data to a list of replication message columns (with values). */
    private List<Column> resolveColumns(Row row, Table table) {
        int numberOfColumns = row.getLengthsCount();
        List<io.debezium.relational.Column> tableColumns = table.columns();
        if (tableColumns.size() != numberOfColumns) {
            throw new IllegalStateException(
                    String.format(
                            "The number of columns in the ROW event %s is different from the in-memory table schema %s.",
                            row,
                            table));
        }

        ByteString rawValues = row.getValues();
        int rawValueIndex = 0;
        List<Column> columns = new ArrayList<>(numberOfColumns);
        for (short i = 0; i < numberOfColumns; i++) {
            final io.debezium.relational.Column column = tableColumns.get(i);
            final String columnName = column.name();
            final VitessType vitessType = new VitessType(column.typeName(), column.jdbcType(), column.enumValues());
            final boolean optional = column.isOptional();

            final int rawValueLength = (int) row.getLengths(i);
            final byte[] rawValue = rawValueLength == -1
                    ? null
                    : rawValues.substring(rawValueIndex, rawValueIndex + rawValueLength).toByteArray();
            if (rawValueLength != -1) {
                // no update to rawValueIndex when no value in the rawValue
                rawValueIndex += rawValueLength;
            }
            columns.add(new ReplicationMessageColumn(columnName, vitessType, optional, rawValue));
        }
        return columns;
    }

    private void handleFieldMessage(Binlogdata.VEvent vEvent, boolean isInVStreamCopy) {
        Binlogdata.FieldEvent fieldEvent = vEvent.getFieldEvent();
        if (fieldEvent == null) {
            LOGGER.error("fieldEvent is expected from {}", vEvent);
        }
        else {
            String[] schemaTableTuple = fieldEvent.getTableName().split("\\.");
            if (schemaTableTuple.length != 2) {
                LOGGER.error(
                        "Handling FIELD VEvent. schemaTableTuple should have schema name and table name but has size {}. {} is skipped",
                        schemaTableTuple.length,
                        vEvent);
            }
            else {
                LOGGER.debug("Handling FIELD VEvent: {}", fieldEvent);
                String schemaName = schemaTableTuple[0];
                String tableName = schemaTableTuple[1];
                String shard = fieldEvent.getShard();
                int columnCount = fieldEvent.getFieldsCount();

                List<ColumnMetaData> columns = new ArrayList<>(columnCount);
                for (short i = 0; i < columnCount; ++i) {
                    Field field = fieldEvent.getFields(i);
                    String columnName = validateColumnName(field.getName(), schemaName, tableName);
                    VitessType vitessType = VitessType.resolve(field, isInVStreamCopy);
                    if (vitessType.getJdbcId() == Types.OTHER) {
                        LOGGER.error("Cannot resolve JDBC type from VStream field {}", field);
                    }

                    KeyMetaData keyMetaData = KeyMetaData.NONE;
                    if ((field.getFlags() & PRI_KEY_FLAG) != 0) {
                        keyMetaData = KeyMetaData.IS_KEY;
                    }
                    else if ((field.getFlags() & UNIQUE_KEY_FLAG) != 0) {
                        keyMetaData = KeyMetaData.IS_UNIQUE_KEY;
                    }
                    boolean optional = (field.getFlags() & NOT_NULL_FLAG) == 0;

                    columns.add(new ColumnMetaData(columnName, vitessType, optional, keyMetaData));
                }

                Table table = resolveTable(shard, schemaName, tableName, columns);
                LOGGER.debug("Number of columns in the resolved table: {}", table.columns().size());

                schema.applySchemaChangesForTable(table);
                return;
            }
        }
    }

    private Table resolveTable(String shard, String schemaName, String tableName, List<ColumnMetaData> columns) {
        List<String> pkColumnNames = new ArrayList<>();
        String uniqueKeyColumnName = null;
        List<io.debezium.relational.Column> cols = new ArrayList<>(columns.size());
        for (ColumnMetaData columnMetaData : columns) {
            ColumnEditor editor = io.debezium.relational.Column.editor()
                    .name(columnMetaData.getColumnName())
                    .type(columnMetaData.getVitessType().getName())
                    .jdbcType(columnMetaData.getVitessType().getJdbcId())
                    .optional(columnMetaData.isOptional());
            VitessType vitessType = columnMetaData.getVitessType();
            if (vitessType.getPrecision().isPresent()) {
                editor = editor.length(vitessType.getPrecision().get());
            }
            if (vitessType.isEnum()) {
                editor = editor.enumValues(vitessType.getEnumValues());
            }
            cols.add(editor.create());

            switch (columnMetaData.getKeyMetaData()) {
                case IS_KEY:
                    pkColumnNames.add(columnMetaData.getColumnName());
                    break;
                case IS_UNIQUE_KEY:
                    if (uniqueKeyColumnName == null) {
                        // use the 1st unique column
                        uniqueKeyColumnName = columnMetaData.getColumnName();
                    }
                    break;
                default:
                    break;
            }
        }

        TableEditor tableEditor = Table
                .editor()
                .addColumns(cols)
                .tableId(VitessDatabaseSchema.buildTableId(shard, schemaName, tableName));

        if (!pkColumnNames.isEmpty()) {
            tableEditor = tableEditor.setPrimaryKeyNames(pkColumnNames);
        }
        else if (uniqueKeyColumnName != null) {
            tableEditor = tableEditor.setPrimaryKeyNames(Collections.singletonList(uniqueKeyColumnName));
        }

        return tableEditor.create();
    }

    @VisibleForTesting
    void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    private static String validateColumnName(String columnName, String schemaName, String tableName) {
        int length = columnName.length();
        if (length == 0) {
            throw new IllegalArgumentException(
                    String.format("Empty column name from schema: %s, table: %s", schemaName, tableName));
        }
        char first = columnName.charAt(0);
        // Vitess VStreamer schema reloading transient bug could cause column names to be anonymized to @1, @2, etc
        // We want to fail in this case instead of sending the corrupted row events with @1, @2 as column names.
        if (first == '@') {
            throw new IllegalArgumentException(
                    String.format("Illegal prefix '@' for column: %s, from schema: %s, table: %s", columnName, schemaName, tableName));
        }
        return columnName;
    }
}
