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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** Vitess Connector entry point */
public class VitessConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnector.class);

    private Map<String, String> properties;
    private VitessConnectorConfig connectorConfig;
    private VitessMetadata vitessMetadata;

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Starting Vitess Connector");
        this.properties = Collections.unmodifiableMap(props);
        this.connectorConfig = new VitessConnectorConfig(Configuration.from(properties));
        this.vitessMetadata = new VitessMetadata(connectorConfig);

    }

    @Override
    public Class<? extends Task> taskClass() {
        return VitessConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Calculating taskConfigs for {} tasks", maxTasks);
        List<String> shards = null;
        if (connectorConfig.offsetStoragePerTask()) {
            shards = connectorConfig.getShard();
            if (shards == null) {
                shards = vitessMetadata.getShards();
            }
        }
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
            int tasks = Math.min(maxTasks, currentShards == null ? Integer.MAX_VALUE : currentShards.size());
            LOGGER.info("There are {} vitess shards for maxTasks: {}, we will use {} tasks",
                    currentShards == null ? null : currentShards.size(), maxTasks, tasks);
            // Check the task offsets persisted from previous gen, we expect the offsets are saved

            VitessOffsetRetriever previousGen = new VitessOffsetRetriever(
                    connectorConfig, prevNumTasks, gen - 1, true, context().offsetStorageReader());

            Map<String, String> prevGtidsPerShard = previousGen.getGtidPerShard();
            validateNoLostShardData(prevGtidsPerShard, currentShards, "gtid positions");
            validateGeneration(prevGtidsPerShard, gen, tasks, prevNumTasks, currentShards);

            VitessOffsetRetriever currentGen = new VitessOffsetRetriever(
                    connectorConfig, tasks, gen, false, context().offsetStorageReader());

            Map<String, String> gtidsPerShard = currentGen.getGtidPerShard();
            validateCurrentGen(currentGen, gtidsPerShard, currentShards, OffsetValueType.GTID);
            List<String> shards = determineShards(prevGtidsPerShard, gtidsPerShard, currentShards);

            if (VitessOffsetRetriever.isShardEpochMapEnabled(connectorConfig)) {
                Map<String, Long> prevEpochsPerShard = previousGen.getEpochPerShard();
                validateNoLostShardData(prevEpochsPerShard, currentShards, "epochs");
                Map<String, Long> epochsPerShard = currentGen.getEpochPerShard();
                validateCurrentGen(currentGen, epochsPerShard, currentShards, OffsetValueType.EPOCH);
                List<String> shardsFromEpoch = determineShards(prevEpochsPerShard, epochsPerShard, currentShards);
                if (!shardsFromEpoch.equals(shards)) {
                    throw new IllegalArgumentException(String.format(
                            "Shards from gtids %s & shards from epoch must be the same %s",
                            shards, shardsFromEpoch));

                }
            }

            // Read GTIDs from config for initial run, only fallback to using this if no stored previous GTIDs, no current GTIDs
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
                taskProps.put(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, String.join(VitessConnectorConfig.CSV_DELIMITER, taskShards));
                taskProps.put(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, Integer.toString(tasks));
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

    private void validateNoLostShardData(Map<String, ?> prevShardToValues, List<String> currentShards, String valueName) {
        if (prevShardToValues != null && !hasSameShards(prevShardToValues.keySet(), currentShards)) {
            LOGGER.warn("Some shards for the previous generation {} are not persisted.  Expected shards: {}",
                    prevShardToValues.keySet(), currentShards);
            if (prevShardToValues.keySet().containsAll(currentShards)) {
                throw new IllegalArgumentException(String.format("Previous shards: %s is the superset of current shards: %s.  "
                        + "We will lose %s for some shards if we continue",
                        prevShardToValues.keySet(), currentShards, valueName));
            }
        }
    }

    private static void validateGeneration(Map<String, String> prevGtidsPerShard, int gen, int tasks, int prevNumTasks, List<String> currentShards) {
        LOGGER.info("Previous gtids Per shard: {}", prevGtidsPerShard);
        Set<String> previousShards = prevGtidsPerShard != null ? prevGtidsPerShard.keySet() : null;
        if (gen > 0 && tasks == prevNumTasks && hasSameShards(previousShards, currentShards)) {
            throw new IllegalArgumentException(String.format(
                    "Previous num.tasks: %s and current num.tasks: %s are the same. "
                            + "And previous shards: %s and current shards: %s are the same. "
                            + "Please choose different tasks.max or have different number of vitess shards "
                            + "if you want to change the task parallelism.  "
                            + "Otherwise please reset the offset.storage.task.key.gen config to its original value",
                    prevNumTasks, tasks, previousShards, currentShards));
        }
    }

    private Map<String, ?> validateCurrentGen(VitessOffsetRetriever retriever, Map<String, ?> valuePerShard, List<String> currentShards,
                                              OffsetValueType valueType) {
        if (valuePerShard != null && !hasSameShards(valuePerShard.keySet(), currentShards)) {
            LOGGER.warn("Some shards {} for the current generation {} are not persisted.  Expected shards: {}",
                    valueType.name(), valuePerShard.keySet(), currentShards);
            if (!currentShards.containsAll(valuePerShard.keySet())) {
                LOGGER.warn("Shards {} from persisted offset: {} not contained within current db shards: {}",
                        valueType.name(), valuePerShard.keySet(), currentShards);
                // valuePerShard has shards not present in the current db shards, we have to rely on the shards
                // from valuePerShard and we have to require all task offsets are persisted.
                retriever.setExpectsOffset(true);
                valuePerShard = retriever.getValuePerShardFromStorage(valueType);
            }
        }
        return valuePerShard;
    }

    /**
     * Use the shards from task offsets persisted in the offset storage if it's not empty.
     * The shards from offset storage might be different than the current db shards. This can happen when
     * Debezium was offline, and a shard split occurred during that time.
     * In this case, we want to use the old shards from the saved offset storage. Those old shards will
     * eventually be replaced with new shards as binlog stream processing handles the shard split event,
     * and new shards will be persisted in offset storage after the shard split event.
     *
     * @param gtidsPerShard     GTIDs per shard.
     * @param prevGtidsPerShard Previous GTIDs per shard.
     * @param currentShards     Current shards.
     * @return List of shards to use.
     */
    private List<String> determineShards(Map<String, ?> prevGtidsPerShard, Map<String, ?> gtidsPerShard, List<String> currentShards) {
        List<String> shards;
        if (gtidsPerShard != null && gtidsPerShard.size() == 0) {
            // if there is no offset persisted for current gen, look for previous gen
            if (prevGtidsPerShard != null && prevGtidsPerShard.size() != 0 && !currentShards.containsAll(prevGtidsPerShard.keySet())) {
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
        return shards;
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
        VitessMetadata testVitessMetadata = new VitessMetadata(connectionConfig);
        try {
            testVitessMetadata.getDatabases();
            LOGGER.info("Successfully tested connection for {}", testVitessMetadata.getConnectionString());
        }
        catch (RuntimeException e) {
            LOGGER.info("Failed testing connection for {} ", testVitessMetadata.getConnectionString());
            hostnameValue.addErrorMessage("Unable to connect: " + e.getMessage());
        }
    }

    public static List<String> getIncludedTables(VitessConnectorConfig connectorConfig, List<String> allTables) {
        // table.include.list are list of patterns, filter all the tables in the keyspace through those patterns
        // to get the list of table names.
        List<String> includedTables = new ArrayList<>();
        String keyspace = connectorConfig.getKeyspace();
        Tables.TableFilter filter = new Filters(connectorConfig).tableFilter();
        for (String table : allTables) {
            if (filter.isIncluded(new TableId("", keyspace, table))) {
                includedTables.add(table);
            }
        }
        return includedTables;
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

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration configuration) {
        VitessConnectorConfig vitessConnectorConfig = new VitessConnectorConfig(configuration);
        String keyspace = vitessConnectorConfig.getKeyspace();
        List<String> allTables = vitessMetadata.getTables();
        List<String> includedTables = getIncludedTables(vitessConnectorConfig, allTables);
        return includedTables.stream()
                .map(table -> new TableId(keyspace, null, table))
                .collect(Collectors.toList());
    }
}
