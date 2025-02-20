/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SERVER;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_DISTINCT_HOSTS;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_SHARD1;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_SHARD2;
import static io.debezium.connector.vitess.VgtidTest.VGTID_JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.util.Collect;
import io.debezium.util.Testing;

public class VitessConnectorTaskTest {

    private static final LogInterceptor logInterceptor = new LogInterceptor(BaseSourceTask.class);
    private static final LogInterceptor vitessLogInterceptor = new LogInterceptor(VitessConnectorTask.class);

    @Test
    public void shouldStartWithTaskOffsetStorageEnabledAndNoOffsets() {
        Configuration config = TestHelper
                .defaultConfig(false, true, 1, 0, 1, null, VitessConnectorConfig.SnapshotMode.NEVER)
                .with(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, "task0_0_1")
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 1)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, "0")
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        ContextHelper helper = new ContextHelper();
        task.initialize(helper.getSourceTaskContext());
        ChangeEventSourceCoordinator coordinator = task.start(config);
        assertThat(vitessLogInterceptor.containsMessage("Using offsets from config")).isTrue();
    }

    @Test
    public void shouldStartWithTaskOffsetStorageDisabledAndNoOffsets() {
        Configuration config = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 1)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, "0")
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        ContextHelper helper = new ContextHelper();
        task.initialize(helper.getSourceTaskContext());
        ChangeEventSourceCoordinator coordinator = task.start(config);
        assertThat(vitessLogInterceptor.containsMessage("No previous offset found")).isTrue();
    }

    @Test
    public void shouldReadOffsetsWhenTaskOffsetStorageDisabled() {
        Configuration config = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 1)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, "0")
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        ContextHelper helper = new ContextHelper();
        helper.storeOffsets(VGTID_JSON, null);
        task.initialize(helper.getSourceTaskContext());
        ChangeEventSourceCoordinator coordinator = task.start(config);
        String expectedMessage = String.format(
                "Found previous partition offset VitessPartition [sourcePartition={server=test_server}]: {vgtid=%s}",
                VGTID_JSON);
        assertThat(logInterceptor.containsMessage(expectedMessage)).isTrue();
    }

    @Test
    public void shouldReadCurrentGenOffsets() {
        String taskKey = VitessConnector.getTaskKeyName(0, 1, 0);
        Configuration config = TestHelper
                .defaultConfig(
                        true,
                        true,
                        1,
                        0,
                        1,
                        null,
                        VitessConnectorConfig.SnapshotMode.NEVER)
                .with(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, taskKey)
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 1)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, "-80,80-")
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        ContextHelper helper = new ContextHelper();
        helper.storeOffsets(null, Map.of(taskKey, VGTID_JSON));
        task.initialize(helper.getSourceTaskContext());
        ChangeEventSourceCoordinator coordinator = task.start(config);
        String expectedMessage = "Using offsets from current gen";
        assertThat(vitessLogInterceptor.containsMessage(expectedMessage)).isTrue();
    }

    @Test
    public void shouldReadPreviousGenOffsets() {
        String taskKeyPrevGen = VitessConnector.getTaskKeyName(0, 1, 0);
        ContextHelper helper = new ContextHelper();
        helper.storeOffsets(null, Map.of(taskKeyPrevGen, VGTID_JSON));

        String taskKey = VitessConnector.getTaskKeyName(0, 2, 1);
        String shards = "-80,80-";
        Configuration config = TestHelper.defaultConfig(
                true,
                true,
                2,
                1,
                1,
                null,
                VitessConnectorConfig.SnapshotMode.NEVER,
                shards)
                .with(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, taskKey)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, shards)
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 2)
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        task.initialize(helper.getSourceTaskContext());
        task.start(config);
        String expectedMessage = "Using offsets from previous gen";
        assertThat(vitessLogInterceptor.containsMessage(expectedMessage)).isTrue();
    }

    @Test
    public void shouldReadPreviousGenOffsetsWhenTasksDecrease() {
        int prevGen = 0;
        int gen = 1;
        int prevNumTasks = 2;
        int numTasks = 1;
        int taskId0 = 0;
        int taskId1 = 1;
        String taskKeyPrevGen0 = VitessConnector.getTaskKeyName(taskId0, prevNumTasks, prevGen);
        String taskKeyPrevGen1 = VitessConnector.getTaskKeyName(taskId1, prevNumTasks, prevGen);
        ContextHelper helper = new ContextHelper();
        helper.storeOffsets(null, Map.of(taskKeyPrevGen0, VGTID_JSON_SHARD1, taskKeyPrevGen1, VGTID_JSON_SHARD2));

        String taskKey = VitessConnector.getTaskKeyName(taskId0, numTasks, gen);
        String shards = "-80,80-";
        Configuration config = TestHelper.defaultConfig(
                true,
                true,
                numTasks,
                gen,
                prevNumTasks,
                null,
                VitessConnectorConfig.SnapshotMode.NEVER,
                shards)
                .with(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, taskKey)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, shards)
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, numTasks)
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        task.initialize(helper.getSourceTaskContext());
        task.start(config);
        Configuration configWithOffsets = task.getConfigWithOffsets(config);
        Vgtid loadedVgtid = Vgtid.of(configWithOffsets.getString(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG));
        assertThat(loadedVgtid).isEqualTo(Vgtid.of(VGTID_JSON_DISTINCT_HOSTS));
    }

    @Test
    public void shouldReadConfiguredOffsets() {
        int gen = 0;
        int numTasks = 1;
        String taskKey = VitessConnector.getTaskKeyName(0, numTasks, gen);
        String shards = "-80,80-";
        Configuration config = TestHelper.defaultConfig(
                true,
                true,
                numTasks,
                gen,
                1,
                null,
                VitessConnectorConfig.SnapshotMode.NEVER,
                shards)
                .with(VitessConnectorConfig.VITESS_TASK_KEY_CONFIG, taskKey)
                .with(VitessConnectorConfig.VITESS_TASK_SHARDS_CONFIG, shards)
                .with(VitessConnectorConfig.VITESS_TOTAL_TASKS_CONFIG, 1)
                .with(VitessConnectorConfig.VGTID, VGTID_JSON)
                .build();
        VitessConnectorTask task = new VitessConnectorTask();
        ContextHelper helper = new ContextHelper();
        task.initialize(helper.getSourceTaskContext());
        task.start(config);
        String expectedMessage = "Using offsets from config";
        assertThat(vitessLogInterceptor.containsMessage(expectedMessage)).isTrue();
    }

    static class ContextHelper {
        OffsetBackingStore offsetStore;
        String engineName = "testOffset";
        SourceTaskContext sourceTaskContext;

        ContextHelper() {
            this.offsetStore = KafkaConnectUtil.memoryOffsetBackingStore();
            this.sourceTaskContext = initSourceTaskContext();
        }

        public SourceTaskContext getSourceTaskContext() {
            return this.sourceTaskContext;
        }

        private SourceTaskContext initSourceTaskContext() {
            offsetStore.start();

            final Converter keyConverter = new JsonConverter();
            Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
            keyConverter.configure(converterConfig, true);
            final Converter valueConverter = new JsonConverter();
            valueConverter.configure(converterConfig, false);
            final OffsetStorageReaderImpl offsetReader = new OffsetStorageReaderImpl(offsetStore, engineName,
                    keyConverter, valueConverter);

            SourceTaskContext sourceTaskContext = new SourceTaskContext() {
                @Override
                public Map<String, String> configs() {
                    return Collections.emptyMap();
                }

                @Override
                public OffsetStorageReader offsetStorageReader() {
                    return offsetReader;
                }

            };
            return sourceTaskContext;
        }

        public void storeOffsets(String serverVgtid, Map<String, Object> prevVgtids) {
            if (serverVgtid == null && (prevVgtids == null || prevVgtids.isEmpty())) {
                Testing.print("Empty gtids to store to offset.");
                return;
            }
            final Converter keyConverter = new JsonConverter();
            Map<String, Object> converterConfig = Collect.hashMapOf("schemas.enable", false);
            keyConverter.configure(converterConfig, true);
            final Converter valueConverter = new JsonConverter();
            valueConverter.configure(converterConfig, false);
            OffsetStorageWriter offsetWriter = new OffsetStorageWriter(offsetStore, engineName,
                    keyConverter, valueConverter);
            if (serverVgtid != null) {
                Testing.print(String.format("Server vgtids: %s", serverVgtid));
                Map<String, Object> sourcePartition = Collect.hashMapOf(
                        VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER);
                Map<String, Object> offset = Collect.hashMapOf(SourceInfo.VGTID_KEY, serverVgtid);
                offsetWriter.offset(sourcePartition, offset);
            }
            if (prevVgtids != null) {
                Testing.print(String.format("Previous vgtids: %s", prevVgtids));
                for (String key : prevVgtids.keySet()) {
                    Map<String, Object> sourcePartition = Collect.hashMapOf(
                            VitessPartition.SERVER_PARTITION_KEY, TEST_SERVER,
                            VitessPartition.TASK_KEY_PARTITION_KEY, key);
                    Map<String, Object> offset = Collect.hashMapOf(SourceInfo.VGTID_KEY, prevVgtids.get(key));
                    offsetWriter.offset(sourcePartition, offset);
                }
            }
            offsetWriter.beginFlush();
            Future<Void> f = offsetWriter.doFlush(null);
            try {
                f.get(100, TimeUnit.MILLISECONDS);
            }
            catch (Exception ex) {
                fail(ex.getMessage());
            }
        }

    }

}
