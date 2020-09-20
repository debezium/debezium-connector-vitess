/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Vitess Connector entry point */
public class VitessConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnector.class);

    private Map<String, String> properties;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Vitess Connector");
        this.properties = Collections.unmodifiableMap(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return VitessConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return VitessConnectorConfig.configDef();
    }

    @Override
    public String version() {
        return Module.version();
    }
}
