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
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.grpc.StatusRuntimeException;

/** Vitess Connector entry point */
public class VitessConnector extends RelationalBaseSourceConnector {

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

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        final VitessConnectorConfig connectionConfig = new VitessConnectorConfig(config);
        try (VitessReplicationConnection connection = new VitessReplicationConnection(connectionConfig, null)) {
            try {
                connection.execute("SHOW DATABASES");
                LOGGER.info("Successfully tested connection for {} with user '{}'", connection.connectionString(), connection.username());
            }
            catch (StatusRuntimeException e) {
                LOGGER.info("Failed testing connection for {} with user '{}'", connection.connectionString(), connection.username());
                hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
            }
        }
        catch (Exception e) {
            LOGGER.error("Unexpected error validating the database connection", e);
            hostnameValue.addErrorMessage("Unable to validate connection: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(VitessConnectorConfig.ALL_FIELDS);
    }
}
