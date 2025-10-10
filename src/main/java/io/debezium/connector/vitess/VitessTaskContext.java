/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.connector.common.CdcSourceTaskContext;

/** A state (context) associated with a Vitess task. Used mostly by metrics collection. */
public class VitessTaskContext extends CdcSourceTaskContext {

    public VitessTaskContext(VitessConnectorConfig config) {
        super(config, getTaskId(config), config.getCustomMetricTags());
    }

    public static String getTaskId(VitessConnectorConfig config) {
        return config.offsetStoragePerTask() ? config.getVitessTaskKey() : "0";
    }
}
