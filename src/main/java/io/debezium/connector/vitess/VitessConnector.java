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
import java.util.Objects;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnectorContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
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
    private VitessConnectorConfig connectorConfig;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Vitess Connector");
        this.properties = Collections.unmodifiableMap(props);
        this.connectorConfig = new VitessConnectorConfig(Configuration.from(properties));

    }

    @Override
    public Class<? extends Task> taskClass() {
        return VitessConnectorTask.class;
    }

    protected Map<String, String> getGtidPerShardFromStorage(int numTasks, int gen, boolean expectsOffset) {
        // Note that in integration test, EmbeddedEngine didn't initialize SourceConnector with SourceConnectorContext
        if (context == null
                || !(context instanceof SourceConnectorContext) || context().offsetStorageReader() == null) {
            LOGGER.warn("Context {} is not setup for the connector, this can happen in unit tests.", context);
            return null;
        }
        final OffsetStorageReader offsetStorageReader = context().offsetStorageReader();
        final Map<String, String> gtidsPerShard = new HashMap<>();
        for (int i = 0; i < numTasks; i++) {
            String taskKey = VitessConnector.getTaskKeyName(i, numTasks, gen);
            VitessPartition par = new VitessPartition(connectorConfig.getLogicalName(), taskKey);
            Map<String, Object> offset = offsetStorageReader.offset(par.getSourcePartition());
            if (offset == null && gen == 0) {
                LOGGER.info("No previous offset for partition: {}, fall back to only server key", par);
                par = new VitessPartition(connectorConfig.getLogicalName(), null);
                offset = offsetStorageReader.offset(par.getSourcePartition());
            }
            if (offset == null) {
                if (expectsOffset) {
                    throw new IllegalArgumentException(String.format("No offset found for %s", par));
                }
                else {
                    LOGGER.warn("No offset found for task key: {}", taskKey);
                    continue;
                }
            }
            final String vgtidStr = (String) offset.get(SourceInfo.VGTID_KEY);
            Objects.requireNonNull(vgtidStr, String.format("No vgtid from %s", offset));
            List<Vgtid.ShardGtid> shardGtids = Vgtid.of(vgtidStr).getShardGtids();
            for (Vgtid.ShardGtid shardGtid : shardGtids) {
                gtidsPerShard.put(shardGtid.getShard(), shardGtid.getGtid());
            }
        }
        return gtidsPerShard;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Calculating taskConfigs for {} tasks", maxTasks);
        List<String> shards = connectorConfig.offsetStoragePerTask() ? getVitessShards(connectorConfig) : null;
        return taskConfigs(maxTasks, shards);
    }

    public List<Map<String, String>> taskConfigs(int maxTasks, List<String> shards) {
        LOGGER.info("Calculating taskConfigs for {} tasks and shards: {}", maxTasks, shards);
        if (connectorConfig.offsetStoragePerTask()) {
            final int prevNumTasks = connectorConfig.getPrevNumTasks();
            final int gen = connectorConfig.getOffsetStorageTaskKeyGen();
            int tasks = Math.min(maxTasks, shards.size());
            LOGGER.info("There are {} vitess shards for maxTasks: {}, we will use {} tasks",
                    shards.size(), maxTasks, tasks);
            if (gen > 0 && tasks == prevNumTasks) {
                throw new IllegalArgumentException(String.format(
                        "Previous num.tasks: %s and current num.tasks: %s are the same. "
                                + "Please choose different tasks.max or have different number of vitess shards "
                                + "if you want to change the task parallelism.  "
                                + "Otherwise please reset the offset.storage.task.key.gen config to its original value",
                        prevNumTasks, tasks));
            }
            Map<String, String> prevGtidsPerShard = gen > 0 ? getGtidPerShardFromStorage(prevNumTasks, gen - 1, true) : null;
            LOGGER.info("Previous gtids Per shard: {}", prevGtidsPerShard);
            if (prevGtidsPerShard != null && prevGtidsPerShard.size() != shards.size()) {
                throw new IllegalArgumentException(String.format(
                        "Different number of shards between offset storage: %s and current vitess shards: %s. ",
                        prevGtidsPerShard, shards));
            }
            final String keyspace = connectorConfig.getKeyspace();
            Map<String, String> gtidsPerShard = getGtidPerShardFromStorage(tasks, gen, false);
            if (gtidsPerShard != null && gtidsPerShard.size() != shards.size()) {
                LOGGER.warn("Some shards for the current generation {} are not persisted.  Expected shards: {}",
                        gtidsPerShard.keySet(), shards);
            }
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
                taskProps.put(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, getTaskKeyName(tid, tasks, gen));
                taskProps.put(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, String.join(",", taskShards));
                List<Vgtid.ShardGtid> shardGtids = new ArrayList<>();
                for (String shard : taskShards) {
                    String gtidStr = gtidsPerShard != null ? gtidsPerShard.get(shard) : null;
                    if (gtidStr == null) {
                        LOGGER.warn("No current gtid found for shard: {}, fallback to previous gen", shard);
                        gtidStr = prevGtidsPerShard != null ? prevGtidsPerShard.get(shard) : null;
                    }
                    if (gtidStr == null) {
                        LOGGER.warn("No previous gtid found either for shard: {}, fallback to current", shard);
                        gtidStr = Vgtid.CURRENT_GTID;
                    }
                    shardGtids.add(new Vgtid.ShardGtid(keyspace, shard, gtidStr));
                }
                taskProps.put(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG, Vgtid.of(shardGtids).toString());
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

    protected static final String getTaskKeyName(int tid, int numTasks, int gen) {
        return String.format("task%d_%d_%d", tid, numTasks, gen);
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
            if (!connectorConfig.offsetStoragePerTask()) {
                String configName = VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name();
                results.computeIfAbsent(configName, k -> new ConfigValue(configName));
                results.get(configName).addErrorMessage(String.format(
                        "%s needs to be enabled when %s > 1", configName, VitessConnectorConfig.TASKS_MAX_CONFIG));
            }
        }
        if (connectorConfig.offsetStoragePerTask()) {
            if (connectorConfig.getOffsetStorageTaskKeyGen() < 0) {
                String configName = VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name();
                results.computeIfAbsent(configName, k -> new ConfigValue(configName));
                results.get(configName).addErrorMessage(String.format(
                        "%s needs to be enabled when %s is specified",
                        configName, VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name()));
            }
        }
        if (connectorConfig.getOffsetStorageTaskKeyGen() >= 0) {
            if (connectorConfig.getPrevNumTasks() <= 0) {
                String configName = VitessConnectorConfig.PREV_NUM_TASKS.name();
                results.computeIfAbsent(configName, k -> new ConfigValue(configName));
                results.get(configName).addErrorMessage(String.format(
                        "%s needs to be enabled when %s is specified",
                        configName, VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name()));
            }
        }
        return results;
    }
}
