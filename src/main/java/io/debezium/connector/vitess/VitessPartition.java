/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class VitessPartition implements Partition {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessPartition.class);

    protected static final String SERVER_PARTITION_KEY = "server";
    protected static final String TASK_KEY_PARTITION_KEY = "task_key";

    private final String serverName;
    private final String taskKeyName;

    public VitessPartition(String serverName, String taskKeyName) {
        this.serverName = serverName;
        this.taskKeyName = taskKeyName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        if (taskKeyName != null) {
            return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName, TASK_KEY_PARTITION_KEY, taskKeyName);
        }
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final VitessPartition other = (VitessPartition) obj;
        return Objects.equals(serverName, other.serverName) && Objects.equals(taskKeyName, other.taskKeyName);
    }

    @Override
    public int hashCode() {
        return serverName.hashCode() ^ (taskKeyName != null ? taskKeyName.hashCode() : 0);
    }

    @Override
    public String toString() {
        return "VitessPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    static class Provider implements Partition.Provider<VitessPartition> {
        private final VitessConnectorConfig connectorConfig;

        Provider(VitessConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<VitessPartition> getPartitions() {
            if (connectorConfig.offsetStoragePerTask()) {
                String taskKey = connectorConfig.getVitessTaskKey();
                if (taskKey == null) {
                    throw new RuntimeException("No vitess.task.key in: {}" + connectorConfig.getConfig());
                }
                return Collections.singleton(new VitessPartition(connectorConfig.getLogicalName(), taskKey));
            }
            else {
                return Collections.singleton(new VitessPartition(connectorConfig.getLogicalName(), null));
            }
        }
    }
}
