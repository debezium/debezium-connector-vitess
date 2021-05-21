/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Optional;
import java.util.Set;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.util.Clock;

/** Always skip snapshot for now */
public class VitessSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<VitessOffsetContext> {

    public VitessSnapshotChangeEventSource(
                                           RelationalDatabaseConnectorConfig connectorConfig,
                                           JdbcConnection jdbcConnection,
                                           EventDispatcher<TableId> dispatcher,
                                           Clock clock,
                                           SnapshotProgressListener snapshotProgressListener) {
        super(
                connectorConfig,
                jdbcConnection,
                dispatcher,
                clock,
                snapshotProgressListener);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<VitessOffsetContext> snapshotContext)
            throws Exception {
        return null;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(
                                               ChangeEventSourceContext sourceContext, RelationalSnapshotContext<VitessOffsetContext> snapshotContext)
            throws Exception {
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<VitessOffsetContext> snapshotContext, VitessOffsetContext offsetContext)
            throws Exception {
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<VitessOffsetContext> snapshotContext,
                                      VitessOffsetContext offsetContext)
            throws Exception {
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<VitessOffsetContext> snapshotContext)
            throws Exception {
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
                                                    RelationalSnapshotContext<VitessOffsetContext> snapshotContext, Table table)
            throws Exception {
        return null;
    }

    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<VitessOffsetContext> snapshotContext, TableId tableId) {
        return Optional.empty();
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(VitessOffsetContext previousOffset) {
        boolean snapshotSchema = false;
        boolean snapshotData = false;
        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<VitessOffsetContext> prepare(ChangeEventSourceContext changeEventSourceContext)
            throws Exception {
        return null;
    }

    @Override
    protected void complete(SnapshotContext<VitessOffsetContext> snapshotContext) {
    }
}
