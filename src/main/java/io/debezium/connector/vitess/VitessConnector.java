/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import io.vitess.proto.Query;
import io.vitess.proto.Vtgate;

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
        LOGGER.info("Calculating taskConfigs for {} tasks", maxTasks);
        List<String> shards = null;
        String storagePerTask = properties.get(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name());
        if (storagePerTask != null && storagePerTask.equalsIgnoreCase("true")) {
            VitessConnectorConfig connectorConfig = new VitessConnectorConfig(Configuration.from(properties));
            shards = getVitessShards(connectorConfig);
        }
        return taskConfigs(maxTasks, shards);
    }

    public List<Map<String, String>> taskConfigs(int maxTasks, List<String> shards) {
        LOGGER.info("Calculating taskConfigs for {} tasks and shards: {}", maxTasks, shards);
        String storagePerTask = properties.get(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name());
        if (storagePerTask != null && storagePerTask.equalsIgnoreCase("true")) {
            int tasks = Math.min(maxTasks, shards.size());
            LOGGER.info("There are {} vitess shards for maxTasks: {}, we will use {} tasks",
                    shards.size(), maxTasks, tasks);
            shards.sort(Comparator.naturalOrder());
            Map<Integer, List<String>> shardsPerTask = new HashMap<>();
            int taskId = 0;
            for (String shard : shards) {
                List<String> taskShards = shardsPerTask.computeIfAbsent(taskId, k -> new ArrayList<>());
                taskShards.add(shard);
                taskId = (taskId + 1) % tasks;
            }
            LOGGER.info("Shards task distribution: {}", shardsPerTask);
            List<Map<String, String>> allTaskProps = new ArrayList<>();
            for (int tid : shardsPerTask.keySet()) {
                List<String> taskShards = shardsPerTask.get(tid);
                Map<String, String> taskProps = new HashMap<>(properties);
                taskProps.put(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, getTaskKeyName(tid, tasks));
                taskProps.put(VitessConnectorConfig.VITESS_TASK_KEY_SHARDS_CONFIG, String.join(",", taskShards));
                allTaskProps.add(taskProps);
            }
            LOGGER.info("taskConfigs are: {}", allTaskProps);
            return allTaskProps;
        }
        else {
            if (maxTasks > 1) {
                throw new IllegalArgumentException("Only a single connector task may be started");
            }
            return Collections.singletonList(properties);
        }
    }

    protected static final String getTaskKeyName(int tid, int numTasks) {
        return String.format("task%d_%d", tid, numTasks);
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

    public static List<String> getVitessShards(VitessConnectorConfig connectionConfig) {
        try (VitessReplicationConnection connection = new VitessReplicationConnection(connectionConfig, null)) {
            String keyspace = connectionConfig.getKeyspace();
            String query = String.format("SHOW VITESS_SHARDS LIKE '%s/%%'", keyspace);
            Vtgate.ExecuteResponse response = connection.execute(query);
            LOGGER.info("Got response {} for keyspace shards", response);
            assert response != null && !response.hasError() && response.hasResult()
                    : String.format("Error response: %s", response);
            Query.QueryResult result = response.getResult();
            List<Query.Row> rows = result.getRowsList();
            assert !rows.isEmpty() : String.format("Empty response: %s", response);
            List<String> shards = new ArrayList<>();
            for (Query.Row row : rows) {
                String fieldValue = row.getValues().toStringUtf8();
                String[] parts = fieldValue.split("/");
                assert parts != null && parts.length == 2 : String.format("Wrong field format: %s", fieldValue);
                shards.add(parts[1]);
            }
            LOGGER.info("Current shards are: {}", shards);
            return shards;
        }
        catch (Exception e) {
            throw new RuntimeException("Unexpected error while retrievign vitess shards", e);
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        LOGGER.info("Validating config: {}", config);
        Map<String, ConfigValue> results = config.validate(VitessConnectorConfig.ALL_FIELDS);
        Integer maxTasks = config.getInteger(VitessConnectorConfig.TASKS_MAX_CONFIG);
        if (maxTasks != null && maxTasks > 1) {
            if (!config.getBoolean(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK)) {
                String offsetConfigName = VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name();
                results.computeIfAbsent(offsetConfigName, k -> new ConfigValue(offsetConfigName));
                ConfigValue offSetConfigValue = results.get(offsetConfigName);
                results.get(offsetConfigName).addErrorMessage(
                        "offset.storage.per.task needs to be enabled when tasks.max > 1");
            }
        }
        return results;
    }
}
