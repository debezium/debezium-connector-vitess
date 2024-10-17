/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.binlog.charset.BinlogCharsetRegistry;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.vitess.connection.DdlMessage;
import io.debezium.connector.vitess.jdbc.VitessBinlogValueConverter;
import io.debezium.connector.vitess.jdbc.VitessDefaultValueConverter;
import io.debezium.relational.HistorizedRelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.ddl.DdlParserListener;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Logical in-memory representation of Vitess schema (a.k.a Vitess keyspace). It is used to create
 * kafka connect {@link Schema} for all tables.
 */
public class VitessDatabaseSchema extends HistorizedRelationalDatabaseSchema {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessDatabaseSchema.class);

    private final DdlParser ddlParser;

    public VitessDatabaseSchema(
                                VitessConnectorConfig config,
                                SchemaNameAdjuster schemaNameAdjuster,
                                TopicNamingStrategy<TableId> topicNamingStrategy) {
        super(
                config,
                topicNamingStrategy,
                new Filters(config).tableFilter(),
                config.getColumnFilter(),
                new TableSchemaBuilder(
                        VitessValueConverter.getInstance(config),
                        new VitessDefaultValueConverter(VitessBinlogValueConverter.getInstance(config)),
                        schemaNameAdjuster,
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getTransactionMetadataFactory().getTransactionStructMaker().getTransactionBlockSchema(),
                        config.getFieldNamer(),
                        false,
                        config.getEventConvertingFailureHandlingMode()),
                false,
                config.getKeyMapper());
        this.ddlParser = new MySqlAntlrDdlParser(
                true,
                false,
                config.isSchemaCommentsHistoryEnabled(),
                getTableFilter(),
                config.getServiceRegistry().getService(BinlogCharsetRegistry.class));
    }

    /** Applies schema changes for the specified table. */
    public void applySchemaChangesForTable(Table table) {

        if (isFilteredOut(table.id())) {
            LOGGER.trace("Skipping schema refresh for table '{}' as table is filtered", table.id());
            return;
        }

        refresh(table);
    }

    private boolean isFilteredOut(TableId id) {
        return !getTableFilter().isIncluded(id);
    }

    /** Refreshes the schema content with a table constructed externally */
    @Override
    public void refresh(Table table) {
        tables().overwriteTable(table);
        refreshSchema(table.id());
    }

    @Override
    public void refreshSchema(TableId id) {
        LOGGER.trace("refreshing DB schema for table '{}'", id);
        Table table = tableFor(id);

        buildAndRegisterSchema(table);
    }

    public static TableId parse(String table) {
        return TableId.parse(table, false);
    }

    /**
     * Create a TableId where we use the shard name as the catalog name.
     * This is needed to ensure that schema updates are applied to each shard individually.
     * This is used in conjunction with {@link VitessTableIdToStringMapper} which excludes the shard/catalog name when
     * determining if a table is in the include/exclude list.
     * @param shard The shard of the table ID to build
     * @param keyspace The keyspace of the table ID to build
     * @param table The table name of the tbale ID to build
     * @return TableId with shard (as catalog), keyspace (as schema), and table
     */
    public static TableId buildTableId(String shard, String keyspace, String table) {
        return new TableId(shard, keyspace, table);
    }

    private String getDatabaseWithShard(String shard, String database) {
        return String.format("%s.%s", shard, database);
    }

    public List<SchemaChangeEvent> parseDdl(VitessPartition partition, VitessOffsetContext offset, DdlMessage ddlMessage, String databaseName) {
        final List<SchemaChangeEvent> schemaChangeEvents = new ArrayList<>(1);
        String ddlStatement = ddlMessage.getStatement();
        String shard = ddlMessage.getShard();
        Instant timestsamp = ddlMessage.getCommitTime();
        DdlChanges ddlChanges = ddlParser.getDdlChanges();
        ddlChanges.reset();
        ddlParser.setCurrentDatabase(getDatabaseWithShard(shard, databaseName));
        ddlParser.parse(ddlStatement, tables());
        if (!ddlChanges.isEmpty()) {
            ddlChanges.getEventsByDatabase((String dbName, List<DdlParserListener.Event> events) -> {
                events.forEach(event -> {
                    final TableId tableId = getTableId(event);
                    offset.event(tableId, timestsamp);
                    SchemaChangeEvent.SchemaChangeEventType type = switch (event.type()) {
                        case CREATE_TABLE -> SchemaChangeEvent.SchemaChangeEventType.CREATE;
                        case DROP_TABLE -> SchemaChangeEvent.SchemaChangeEventType.DROP;
                        case ALTER_TABLE-> SchemaChangeEvent.SchemaChangeEventType.ALTER;
                        case TRUNCATE_TABLE -> SchemaChangeEvent.SchemaChangeEventType.TRUNCATE;
                        default -> {
                            LOGGER.info("Skipped DDL event type {}: {}", event.type(), ddlStatement);
                            yield null;
                        }
                    };
                    emitChangeEvent(
                            partition,
                            offset,
                            schemaChangeEvents,
                            databaseName,
                            event,
                            tableId,
                            type,
                            false);
                });
            });
            return schemaChangeEvents;
        }
        return Collections.emptyList();
    }

    private TableId getTableId(DdlParserListener.Event event) {
        if (event instanceof DdlParserListener.TableEvent) {
            DdlParserListener.TableEvent tableEvent = (DdlParserListener.TableEvent) event;
            return tableEvent.tableId();
        }
        return null;
    }

    private void emitChangeEvent(VitessPartition partition, VitessOffsetContext offset, List<SchemaChangeEvent> schemaChangeEvents,
                                 String sanitizedDbName, DdlParserListener.Event event, TableId tableId,
                                 SchemaChangeEvent.SchemaChangeEventType type, boolean snapshot) {
        SchemaChangeEvent schemaChangeEvent;
        if (type.equals(SchemaChangeEvent.SchemaChangeEventType.ALTER) && event instanceof DdlParserListener.TableAlteredEvent
                && ((DdlParserListener.TableAlteredEvent) event).previousTableId() != null) {
            schemaChangeEvent = SchemaChangeEvent.ofRename(
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    tableId != null ? tables().forTable(tableId) : null,
                    ((DdlParserListener.TableAlteredEvent) event).previousTableId());
        }
        else {
            Table table = getTable(tableId, type);
            schemaChangeEvent = SchemaChangeEvent.of(
                    type,
                    partition,
                    offset,
                    sanitizedDbName,
                    null,
                    event.statement(),
                    table,
                    snapshot);
        }
        schemaChangeEvents.add(schemaChangeEvent);
    }

    private Table getTable(TableId tableId, SchemaChangeEvent.SchemaChangeEventType type) {
        if (tableId == null) {
            return null;
        }
        if (SchemaChangeEvent.SchemaChangeEventType.DROP == type) {
            // DROP events don't have information about tableChanges, so we are creating a Table object
            // with just the tableId to be used during blocking snapshot to filter out drop events not
            // related to table to be snapshotted.
            return Table.editor().tableId(tableId).create();
        }
        return tables().forTable(tableId);
    }

    public DdlParser getDdlParser() {
        return ddlParser;
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent schemaChange) {
        switch (schemaChange.getType()) {
            case CREATE:
            case ALTER:
                schemaChange.getTableChanges().forEach(x -> buildAndRegisterSchema(x.getTable()));
                break;
            case DROP:
                schemaChange.getTableChanges().forEach(x -> removeSchema(x.getId()));
                break;
            default:
        }

        // Record the DDL statement so that we can later recover them.
        // This is done _after_ writing the schema change records so that failure recovery (which is based on
        // the schema history) won't lose schema change records.
        //
        // We either store:
        // - all DDLs if configured
        // - or global SEt variables
        // - or DDLs for captured objects
        if (!storeOnlyCapturedTables() || isGlobalSetVariableStatement(schemaChange.getDdl(), schemaChange.getDatabase())
                || schemaChange.getTables().stream().map(Table::id).anyMatch(getTableFilter()::isIncluded)) {
            LOGGER.debug("Recorded DDL statements for database '{}': {}", schemaChange.getDatabase(), schemaChange.getDdl());
            record(schemaChange, schemaChange.getTableChanges());
        }

    }

    public boolean isGlobalSetVariableStatement(String ddl, String databaseName) {
        return (databaseName == null || databaseName.isEmpty()) && ddl != null && ddl.toUpperCase().startsWith("SET ");
    }
}
