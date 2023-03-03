/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.connector.vitess.connection.ReplicationConnection;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/**
 * Factory to create StreamingChangeEventSource and SnapshotChangeEventSource. A dummy
 * SnapshotChangeEventSource is created because snapshot is not supported for now.
 */
public class VitessChangeEventSourceFactory implements ChangeEventSourceFactory<VitessPartition, VitessOffsetContext> {

    private final VitessConnectorConfig connectorConfig;
    private final ErrorHandler errorHandler;
    private final EventDispatcher<VitessPartition, TableId> dispatcher;
    private final Clock clock;
    private final VitessDatabaseSchema schema;
    private final ReplicationConnection replicationConnection;

    public VitessChangeEventSourceFactory(
                                          VitessConnectorConfig connectorConfig,
                                          ErrorHandler errorHandler,
                                          EventDispatcher<VitessPartition, TableId> dispatcher,
                                          Clock clock,
                                          VitessDatabaseSchema schema,
                                          ReplicationConnection replicationConnection) {
        this.connectorConfig = connectorConfig;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.replicationConnection = replicationConnection;
    }

    @Override
    public SnapshotChangeEventSource<VitessPartition, VitessOffsetContext> getSnapshotChangeEventSource(SnapshotProgressListener<VitessPartition> snapshotProgressListener) {
        // A dummy SnapshotChangeEventSource, snapshot is skipped.
        return new VitessSnapshotChangeEventSource(
                connectorConfig, new DefaultMainConnectionProvidingConnectionFactory<>(() -> null), dispatcher, schema, clock, null);
    }

    @Override
    public StreamingChangeEventSource<VitessPartition, VitessOffsetContext> getStreamingChangeEventSource() {
        return new VitessStreamingChangeEventSource(
                dispatcher,
                errorHandler,
                clock,
                schema,
                connectorConfig,
                replicationConnection);
    }
}
