/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.sql.SQLException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.connection.DdlMessage;
import io.debezium.connector.vitess.jdbc.VitessConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/** Always skip snapshot for now */
public class VitessSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<VitessPartition, VitessOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessSnapshotChangeEventSource.class);

    private final VitessConnectorConfig connectorConfig;
    private final VitessDatabaseSchema schema;
    private final VitessConnection connection;
    private List<String> shards;

    public VitessSnapshotChangeEventSource(
                                           VitessConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<VitessConnection> connectionFactory,
                                           EventDispatcher<VitessPartition, TableId> dispatcher,
                                           VitessDatabaseSchema schema,
                                           Clock clock,
                                           SnapshotProgressListener<VitessPartition> snapshotProgressListener,
                                           NotificationService<VitessPartition, VitessOffsetContext> notificationService, SnapshotterService snapshotterService) {
        super(
                connectorConfig,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService,
                snapshotterService);
        this.connectorConfig = connectorConfig;
        this.connection = connectionFactory.mainConnection();
        this.schema = schema;
        this.shards = new VitessMetadata(connectorConfig).getShards();
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
        Set<TableId> tableIds = new HashSet<>();
        try {
            connection.query("SHOW TABLES", rs -> {
                while (rs.next()) {
                    for (String shard : shards) {
                        TableId id = new TableId(shard, connectorConfig.getKeyspace(), rs.getString(1));
                        tableIds.add(id);
                    }
                }
            });
        }
        catch (SQLException e) {
            LOGGER.warn("\t skipping database '{}' due to error reading tables: {}", connectorConfig.getKeyspace(), e.getMessage());
        }
        LOGGER.debug("All table IDs {}", tableIds);
        return tableIds;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(
                                               ChangeEventSourceContext sourceContext, RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> context, VitessOffsetContext previousOffset) {
        context.offset = VitessOffsetContext.initialContext(connectorConfig, Clock.system());
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext,
                                      VitessOffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        Set<TableId> capturedSchemaTables = snapshotContext.capturedSchemaTables;

        for (TableId tableId : capturedSchemaTables) {
            String sql = "SHOW CREATE TABLE " + quote(tableId);
            connection.query(sql, rs -> {
                if (rs.next()) {
                    String ddlStatement = rs.getString(2);
                    for (String shard : shards) {
                        snapshotContext.offset.setShard(shard);
                        DdlMessage ddlMessage = new DdlMessage("", Instant.now(), ddlStatement, shard);
                        List<SchemaChangeEvent> schemaChangeEvents = schema.parseDdl(
                                snapshotContext.partition, snapshotContext.offset, ddlMessage, connectorConfig.getKeyspace());
                        for (SchemaChangeEvent schemaChangeEvent : schemaChangeEvents) {
                            LOGGER.info("Adding schema change event {}", schemaChangeEvent);
                            Table table = schema.tableFor(tableId);
                            if (table != null) {
                                LOGGER.info("Adding schema for table {}", table.id());
                                Table updatedTable = getTableWithShardAsCatalog(table, shard);
                                snapshotContext.tables.overwriteTable(updatedTable);
                            }
                        }
                    }
                }
            });
        }
    }

    private Table getTableWithShardAsCatalog(Table table, String shard) {
        String[] shardAndDatabase = table.id().catalog().split("\\.");
        String database = shardAndDatabase[1];
        String tableName = table.id().table();
        return table.edit().tableId(new TableId(shard, database, tableName)).create();
    }

    private String quote(TableId id) {
        return quote(id.schema()) + "." + quote(id.table());
    }

    private String quote(String dbOrTableName) {
        return "`" + dbOrTableName + "`";
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext,
                                                    Table table) {
        return SchemaChangeEvent.ofSnapshotCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table);
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext, TableId tableId, List<String> columns) {
        return Optional.empty();
    }

    @Override
    protected SnapshotContext<VitessPartition, VitessOffsetContext> prepare(VitessPartition partition, boolean onDemand) {
        return new RelationalSnapshotContext<>(partition, connectorConfig.getKeyspace(), onDemand);
    }

    @Override
    protected VitessOffsetContext copyOffset(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
        return null;
    }

}
