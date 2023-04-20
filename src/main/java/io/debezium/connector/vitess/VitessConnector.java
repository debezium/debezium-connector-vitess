/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import io.debezium.util.Strings;
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

    public static boolean hasSameShards(Collection<String> shardsOne, Collection<String> shardsTwo) {
        if (shardsOne == null) {
            return shardsTwo == null;
        }
        else if (shardsTwo == null) {
            return shardsOne == null;
        }
        else if (shardsOne.size() != shardsTwo.size()) {
            return false;
        }
        else {
            // Order independent comparison
            Set<String> setOne = new HashSet<>(shardsOne);
            Set<String> setTwo = new HashSet<>(shardsTwo);
            return setOne.equals(setTwo);
        }
    }

    public List<Map<String, String>> taskConfigs(int maxTasks, List<String> currentShards) {
        LOGGER.info("Calculating taskConfigs for {} tasks and shards: {}", maxTasks, currentShards);
        if (connectorConfig.offsetStoragePerTask()) {
            final int prevNumTasks = connectorConfig.getPrevNumTasks();
            final int gen = connectorConfig.getOffsetStorageTaskKeyGen();
            int tasks = Math.min(maxTasks, currentShards.size());
            LOGGER.info("There are {} vitess shards for maxTasks: {}, we will use {} tasks",
                    currentShards.size(), maxTasks, tasks);
            if (gen > 0 && tasks == prevNumTasks) {
                throw new IllegalArgumentException(String.format(
                        "Previous num.tasks: %s and current num.tasks: %s are the same. "
                                + "Please choose different tasks.max or have different number of vitess shards "
                                + "if you want to change the task parallelism.  "
                                + "Otherwise please reset the offset.storage.task.key.gen config to its original value",
                        prevNumTasks, tasks));
            }
            // Check the task offsets persisted from previous gen, we expect the offsets are saved
            Map<String, String> prevGtidsPerShard = gen > 0 ? getGtidPerShardFromStorage(prevNumTasks, gen - 1, true) : null;
            LOGGER.info("Previous gtids Per shard: {}", prevGtidsPerShard);
            if (prevGtidsPerShard != null && !hasSameShards(prevGtidsPerShard.keySet(), currentShards)) {
                LOGGER.warn("Some shards for the previous generation {} are not persisted.  Expected shards: {}",
                        prevGtidsPerShard.keySet(), currentShards);
            }
            final String keyspace = connectorConfig.getKeyspace();
            // Check the configs in case there is a user specified GTID override
            verifyShardGtidConfig();
            Map<String, String> gtidsPerShard = getGtidsPerShardFromConfig();
            // Check the task offsets for the current gen, the offset might not be persisted if this gen just turned on
            if (gtidsPerShard == null) {
                gtidsPerShard = getGtidPerShardFromStorage(tasks, gen, false);
                if (gtidsPerShard != null && !hasSameShards(gtidsPerShard.keySet(), currentShards)) {
                    LOGGER.warn("Some shards for the current generation {} are not persisted.  Expected shards: {}",
                            gtidsPerShard.keySet(), currentShards);
                    if (!currentShards.containsAll(gtidsPerShard.keySet())) {
                        LOGGER.warn("Shards from persisted offset: {} not contained within current db shards: {}",
                                gtidsPerShard.keySet(), currentShards);
                        // gtidsPerShard has shards not present in the current db shards, we have to rely on the shards
                        // from gtidsPerShard and we have to require all task offsets are persisted.
                        gtidsPerShard = getGtidPerShardFromStorage(tasks, gen, true);
                    }
                }
            }
            // Use the shards from task offsets persisted in the offset storage if it's not empty.
            // The shards from offset storage might be different than the current db shards, this can happen when
            // debezium was offline and there was a shard split happened during that time.
            // In this case we want to use the old shards from the saved offset storage. Those old shards will
            // eventually be replaced with new shards as binlog stream processing handles the shard split event
            // and new shards will be persisted in offset storage after the shard split event.
            List<String> shards = null;
            if (gtidsPerShard != null && gtidsPerShard.size() == 0) {
                // if there is no offset persisted for current gen, look for previous gen
                if (prevGtidsPerShard != null && prevGtidsPerShard.size() != 0) {
                    LOGGER.info("Using shards from persisted offset from prev gen: {}", prevGtidsPerShard.keySet());
                    shards = new ArrayList<>(prevGtidsPerShard.keySet());
                }
                else {
                    LOGGER.warn("No persisted offset for current or previous gen, using current shards from db: {}", currentShards);
                    shards = currentShards;
                }
            }
            else if (gtidsPerShard != null && !currentShards.containsAll(gtidsPerShard.keySet())) {
                LOGGER.info("Persisted offset has different shards, Using shards from persisted offset: {}", gtidsPerShard.keySet());
                shards = new ArrayList<>(gtidsPerShard.keySet());
            }
            else {
                // In this case, we prefer the current db shards since gtidsPerShard might only be partially persisted
                LOGGER.warn("Current db shards is the superset of persisted offset, using current shards from db: {}", currentShards);
                shards = currentShards;
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
                        gtidStr = connectorConfig.getGtid().get(0);
                        LOGGER.warn("No previous gtid found either for shard: {}, fallback to '{}'", shard, gtidStr);
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
            verifyShardGtidConfig();
            return Collections.singletonList(properties);
        }
    }

    private Map<String, String> getGtidsPerShardFromConfig() {
        Map<String, String> gtidsPerShard = null;
        if (connectorConfig.getShard() != null && connectorConfig.getGtid().size() > 1) {
            gtidsPerShard = IntStream.range(0, connectorConfig.getShard().size())
                    .boxed()
                    .collect(Collectors.toMap(connectorConfig.getShard()::get, connectorConfig.getGtid()::get));
            LOGGER.info("Found GTIDs per shard in config {}", gtidsPerShard);
        }
        return gtidsPerShard;
    }

    private void verifyShardGtidConfig() {
        final List<String> gtids = connectorConfig.getGtid();
        if (connectorConfig.getShard() != null &&
                gtids != VitessConnectorConfig.DEFAULT_GTID_LIST &&
                gtids != VitessConnectorConfig.EMPTY_GTID_LIST &&
                connectorConfig.getShard().size() != connectorConfig.getGtid().size()) {
            throw new IllegalArgumentException("If GTIDs are specified must be specified for all shards");
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

    private static List<String> getRowsFromQuery(VitessConnectorConfig connectionConfig, String query) {
        try (VitessReplicationConnection connection = new VitessReplicationConnection(connectionConfig, null)) {
            Vtgate.ExecuteResponse response = connection.execute(query);
            LOGGER.info("Got response: {} for query: {}", response, query);
            assert response != null && !response.hasError() && response.hasResult()
                    : String.format("Error response: %s", response);
            Query.QueryResult result = response.getResult();
            List<Query.Row> rows = result.getRowsList();
            assert !rows.isEmpty() : String.format("Empty response: %s", response);
            return rows.stream().map(s -> s.getValues().toStringUtf8()).collect(Collectors.toList());
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Unexpected error while running query: %s", query), e);
        }
    }

    public static List<String> getIncludedTables(String keyspace, String tableIncludeList, List<String> allTables) {
        // table.include.list are list of patterns, filter all the tables in the keyspace through those patterns
        // to get the list of table names.
        final List<Pattern> patterns = Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE);
        List<String> includedTables = new ArrayList<>();
        for (String ksTable : allTables) {
            for (Pattern pattern : patterns) {
                if (pattern.asPredicate().test(String.format("%s.%s", keyspace, ksTable))) {
                    includedTables.add(ksTable);
                    break;
                }
            }
        }
        return includedTables;
    }

    public static List<String> getKeyspaceTables(VitessConnectorConfig connectionConfig) {
        String query = String.format("SHOW TABLES FROM %s", connectionConfig.getKeyspace());
        List<String> tables = getRowsFromQuery(connectionConfig, query);
        LOGGER.info("All tables from keyspace {} are: {}", connectionConfig.getKeyspace(), tables);
        return tables;
    }

    public static List<String> getVitessShards(VitessConnectorConfig connectionConfig) {
        String query = String.format("SHOW VITESS_SHARDS LIKE '%s/%%'", connectionConfig.getKeyspace());
        List<String> rows = getRowsFromQuery(connectionConfig, query);
        List<String> shards = rows.stream().map(fieldValue -> {
            String[] parts = fieldValue.split("/");
            assert parts != null && parts.length == 2 : String.format("Wrong field format: %s", fieldValue);
            return parts[1];
        }).collect(Collectors.toList());
        LOGGER.info("Shards: {}", shards);
        return shards;
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        LOGGER.info("Validating config: {}", config);
        Map<String, ConfigValue> results = config.validate(VitessConnectorConfig.ALL_FIELDS);
        Integer maxTasks = config.getInteger(VitessConnectorConfig.TASKS_MAX_CONFIG);
        VitessConnectorConfig tempConnectorConfig = new VitessConnectorConfig(config);
        if (maxTasks != null && maxTasks > 1) {
            if (!tempConnectorConfig.offsetStoragePerTask()) {
                String configName = VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name();
                results.computeIfAbsent(configName, k -> new ConfigValue(configName));
                results.get(configName).addErrorMessage(String.format(
                        "%s needs to be enabled when %s > 1", configName, VitessConnectorConfig.TASKS_MAX_CONFIG));
            }
        }
        if (tempConnectorConfig.offsetStoragePerTask()) {
            if (tempConnectorConfig.getOffsetStorageTaskKeyGen() < 0) {
                String configName = VitessConnectorConfig.OFFSET_STORAGE_TASK_KEY_GEN.name();
                results.computeIfAbsent(configName, k -> new ConfigValue(configName));
                results.get(configName).addErrorMessage(String.format(
                        "%s needs to be enabled when %s is specified",
                        configName, VitessConnectorConfig.OFFSET_STORAGE_PER_TASK.name()));
            }
        }
        if (tempConnectorConfig.getOffsetStorageTaskKeyGen() >= 0) {
            if (tempConnectorConfig.getPrevNumTasks() <= 0) {
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
