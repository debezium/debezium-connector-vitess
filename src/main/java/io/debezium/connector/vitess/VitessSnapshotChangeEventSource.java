/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

/** Always skip snapshot for now */
public class VitessSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<VitessPartition, VitessOffsetContext> {

    public VitessSnapshotChangeEventSource(
                                           RelationalDatabaseConnectorConfig connectorConfig,
                                           MainConnectionProvidingConnectionFactory<JdbcConnection> connectionFactory,
                                           EventDispatcher<VitessPartition, TableId> dispatcher,
                                           VitessDatabaseSchema schema,
                                           Clock clock,
                                           SnapshotProgressListener<VitessPartition> snapshotProgressListener,
                                           NotificationService<VitessPartition, VitessOffsetContext> notificationService) {
        super(
                connectorConfig,
                connectionFactory,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
        return null;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(
                                               ChangeEventSourceContext sourceContext, RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext, VitessOffsetContext offsetContext) {
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext,
                                      VitessOffsetContext offsetContext, SnapshottingTask snapshottingTask) {
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext,
                                                    Table table) {
        return null;
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext, TableId tableId, List<String> columns) {
        return Optional.empty();
    }

    @Override
    public SnapshottingTask getSnapshottingTask(VitessPartition partition, VitessOffsetContext previousOffset) {
        boolean snapshotSchema = false;
        boolean snapshotData = false;
        return new SnapshottingTask(snapshotSchema, snapshotData, List.of(), Map.of(), false);
    }

    @Override
    protected SnapshotContext<VitessPartition, VitessOffsetContext> prepare(VitessPartition partition, boolean onDemand) {
        return new RelationalSnapshotContext<>(partition, "", onDemand);
    }

    @Override
    protected VitessOffsetContext copyOffset(RelationalSnapshotContext<VitessPartition, VitessOffsetContext> snapshotContext) {
        return null;
    }

}
