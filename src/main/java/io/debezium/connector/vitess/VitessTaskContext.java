/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Map;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.util.Collect;

/** A state (context) associated with a Vitess task. Used mostly by metrics collection. */
public class VitessTaskContext extends CdcSourceTaskContext {

    private final VitessConnectorConfig config;

    public VitessTaskContext(VitessConnectorConfig config, VitessDatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
        this.config = config;
    }

    @Override
    public Map<String, String> getConnectorProperties() {
        Map<String, String> props = Collect.hashMapOf("keyspace", config.getKeyspace());
        String shard = config.getShard();
        if (shard != null && !shard.isEmpty()) {
            props.put("shard", shard);
        }
        return props;
    }
}
