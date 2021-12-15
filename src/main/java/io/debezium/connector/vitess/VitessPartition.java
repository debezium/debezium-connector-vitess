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

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class VitessPartition implements Partition {
    private static final String SERVER_KEY = "server";
    private static final String SHARD_KEY = "shard";

    private final String serverName;
    private final String shard;

    public VitessPartition(String serverName) {
        this.serverName = serverName;
        this.shard = null;
    }

    public VitessPartition(String serverName, String shard) {
        this.serverName = serverName;
        this.shard = shard;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        if (shard == null) {
            return Collect.hashMapOf(SERVER_KEY, serverName);
        }
        return Collect.hashMapOf(SERVER_KEY, serverName, SHARD_KEY, shard);
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
        return Objects.equals(serverName, other.serverName) && Objects.equals(shard, other.shard);
    }

    @Override
    public int hashCode() {
        if (shard == null) {
            return serverName.hashCode();
        }
        return Objects.hash(serverName, shard);
    }

    static class Provider implements Partition.Provider<VitessPartition> {
        private final VitessConnectorConfig connectorConfig;

        Provider(VitessConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<VitessPartition> getPartitions() {
            String shard = connectorConfig.getShard();
            return Collections.singleton(new VitessPartition(connectorConfig.getLogicalName(), shard));
        }
    }
}
