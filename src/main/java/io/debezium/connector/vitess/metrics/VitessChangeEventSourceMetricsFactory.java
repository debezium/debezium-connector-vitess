/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.vitess.VitessPartition;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Chris Cranford
 */
public class VitessChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<VitessPartition> {

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<VitessPartition> getSnapshotMetrics(T taskContext,
                                                                                                                 ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                 EventMetadataProvider eventMetadataProvider) {
        return new VitessSnapshotChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<VitessPartition> getStreamingMetrics(T taskContext,
                                                                                                                   ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                   EventMetadataProvider eventMetadataProvider) {
        return new VitessStreamingChangeEventSourceMetrics(taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }
}
