/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.metrics;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.vitess.VitessPartition;
import io.debezium.pipeline.metrics.DefaultSnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Collect;

/**
 * @author Henry Haiying Cai
 */
@ThreadSafe
public class VitessSnapshotChangeEventSourceMetrics extends DefaultSnapshotChangeEventSourceMetrics<VitessPartition> {

    public <T extends CdcSourceTaskContext> VitessSnapshotChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                   EventMetadataProvider metadataProvider) {
        super(taskContext, changeEventQueueMetrics, metadataProvider,
                Collect.linkMapOf("context", "snapshot", "server", taskContext.getConnectorLogicalName(), "task", taskContext.getTaskId()));
    }
}
