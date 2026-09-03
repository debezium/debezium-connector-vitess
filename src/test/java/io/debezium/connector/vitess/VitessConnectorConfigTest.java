/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SHARD_TO_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;

public class VitessConnectorConfigTest {

    @Test
    public void shouldGetVitessHeartbeatImplWhenIntervalSet() {
        Configuration configuration = TestHelper.defaultConfig().with(
                Heartbeat.HEARTBEAT_INTERVAL, 1000).build();

        ScheduledHeartbeat heartbeat = new VitessHeartbeatFactory().getScheduledHeartbeat(
                new VitessConnectorConfig(configuration),
                null,
                null,
                null);

        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat instanceof VitessHeartbeatImpl).isTrue();
    }

    @Test
    public void shouldGetVitessHeartbeatNoOp() {
        Configuration configuration = TestHelper
                .defaultConfig()
                .build();

        ScheduledHeartbeat heartbeat = new VitessHeartbeatFactory().getScheduledHeartbeat(
                new VitessConnectorConfig(configuration),
                null,
                null,
                null);

        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat).isEqualTo(ScheduledHeartbeat.NOOP_HEARTBEAT);
    }

    @Test
    public void shouldExcludeEmptyShards() {
        Configuration configuration = TestHelper.defaultConfig().with(
                VitessConnectorConfig.EXCLUDE_EMPTY_SHARDS, true).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.excludeEmptyShards()).isTrue();
    }

    @Test
    public void shouldGetVitessTaskEpochShardMapConfig() {
        Configuration configuration = TestHelper.defaultConfig().with(
                VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG, TEST_SHARD_TO_EPOCH.toString()).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getVitessTaskShardEpochMap()).isEqualTo(TEST_SHARD_TO_EPOCH);
    }

    @Test
    public void shouldGetVitessEpochShardMapConfig() {
        Configuration configuration = TestHelper.defaultConfig().with(
                VitessConnectorConfig.SHARD_EPOCH_MAP, TEST_SHARD_TO_EPOCH.toString()).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getShardEpochMap()).isEqualTo(TEST_SHARD_TO_EPOCH.toString());
    }

    @Test
    public void shouldGetVitessEpochShardMapConfigDefault() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getShardEpochMap()).isEqualTo("");
    }

    @Test
    public void shouldInvalidLoadBalancerPolicyFailValidation() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY, "foo").build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY), printConsumer);
        assertThat(inputs.size()).isEqualTo(1);
    }

    @Test
    public void shouldRoundRobinLoadBalancerPolicyPassValidation() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY, "round_robin").build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY), printConsumer);
        assertThat(inputs.size()).isEqualTo(0);
    }

    @Test
    public void shouldPickFirstLoadBalancerPolicyPassValidation() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY, "pick_first").build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY), printConsumer);
        assertThat(inputs.size()).isEqualTo(0);
    }

    @Test
    public void shouldDefaultLoadBalancerPolicyPassValidation() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.GRPC_DEFAULT_LOAD_BALANCING_POLICY), printConsumer);
        assertThat(inputs.size()).isEqualTo(0);
    }

    @Test
    public void shouldImproperShardEpochMapFailValidation() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.SHARD_EPOCH_MAP, "foo").build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.SHARD_EPOCH_MAP), printConsumer);
        assertThat(inputs.size()).isEqualTo(1);
    }

    @Test
    public void shouldEnableInheritEpoch() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.INHERIT_EPOCH, true).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getInheritEpoch()).isTrue();
    }

    @Test
    public void shouldValidateInheritEpochWithoutOrderedTransactionMetadata() {
        Configuration configuration = TestHelper.defaultConfig().with(VitessConnectorConfig.INHERIT_EPOCH, true).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.INHERIT_EPOCH), printConsumer);
        assertThat(inputs.size()).isEqualTo(1);
    }

    @Test
    public void shouldValidateInheritEpochWithOrderedTransactionMetadata() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.INHERIT_EPOCH, true)
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> inputs = new ArrayList<>();
        Consumer<String> printConsumer = (input) -> {
            inputs.add(input);
        };
        connectorConfig.validateAndRecord(List.of(VitessConnectorConfig.INHERIT_EPOCH), printConsumer);
        assertThat(inputs.size()).isEqualTo(0);
    }

    @Test
    public void shouldEnableStreamKeyspaceHeartbeatsConfig() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.STREAM_KEYSPACE_HEARTBEATS, true)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getStreamKeyspaceHeartbeats()).isTrue();
    }

    @Test
    public void shouldEnableExcludeKeyspaceFromTableNameConfig() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.EXCLUDE_KEYSPACE_FROM_TABLE_NAME, true)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getExcludeKeyspaceFromTableName()).isTrue();
    }

    @Test
    public void shouldExcludeKeyspaceFromTableNameConfigDefaultToFalse() {
        Configuration configuration = TestHelper.defaultConfig()
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getExcludeKeyspaceFromTableName()).isFalse();
    }

    @Test
    public void shouldDefaultDisableStreamKeyspaceHeartbeatsConfig() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getStreamKeyspaceHeartbeats()).isFalse();
    }

    @Test
    public void shouldFilterTablesToCopyWithSingleRegexPattern() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, TestHelper.TEST_UNSHARDED_KEYSPACE + ".numeric_.*")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> allTables = Arrays.asList("numeric_table", "numeric_table2", "string_table", "enum_table");
        List<String> tablesToCopy = VitessConnector.getTablesToCopyByPrefix(connectorConfig, allTables);
        assertThat(tablesToCopy).containsExactlyInAnyOrder("numeric_table", "numeric_table2");
    }

    @Test
    public void shouldFilterTablesToCopyWithMultipleRegexPatterns() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, TestHelper.TEST_UNSHARDED_KEYSPACE + ".numeric_.*," + TestHelper.TEST_UNSHARDED_KEYSPACE + ".string_.*")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> allTables = Arrays.asList("numeric_table", "numeric_table2", "string_table", "string_table2", "enum_table");
        List<String> tablesToCopy = VitessConnector.getTablesToCopyByPrefix(connectorConfig, allTables);
        assertThat(tablesToCopy).containsExactlyInAnyOrder("numeric_table", "numeric_table2", "string_table", "string_table2");
    }

    @Test
    public void shouldReturnEmptyListWhenSnapshotModeTablesNotSet() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> allTables = Arrays.asList("numeric_table", "string_table");
        List<String> tablesToCopy = VitessConnector.getTablesToCopyByPrefix(connectorConfig, allTables);
        assertThat(tablesToCopy).isEmpty();
    }

    @Test
    public void shouldReturnEmptyListWhenSnapshotModeTablesIsEmptyString() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, "")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> allTables = Arrays.asList("numeric_table", "string_table", "enum_table");
        List<String> tablesToCopy = VitessConnector.getTablesToCopyByPrefix(connectorConfig, allTables);
        assertThat(tablesToCopy).isEmpty();
    }

    @Test
    public void shouldFilterTablesToCopyWithExactTableName() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(CommonConnectorConfig.SNAPSHOT_MODE_TABLES, TestHelper.TEST_UNSHARDED_KEYSPACE + ".numeric_table")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        List<String> allTables = Arrays.asList("numeric_table", "numeric_table2", "string_table");
        List<String> tablesToCopy = VitessConnector.getTablesToCopyByPrefix(connectorConfig, allTables);
        assertThat(tablesToCopy).containsExactly("numeric_table");
    }

    @Test
    public void shouldGetConnectorGeneration() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.CONNECTOR_GENERATION, 5)
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getConnectorGeneration()).isEqualTo(5);
    }

    @Test
    public void shouldGetConnectorGenerationDefaultValue() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getConnectorGeneration()).isEqualTo(0);
    }

    @Test
    @FixFor("debezium/dbz#2479")
    public void shouldFailInheritEpochValidationWithoutOrderedTransactionMetadataFactory() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.INHERIT_EPOCH, true)
                .build();
        List<String> problems = new ArrayList<>();
        boolean valid = VitessConnectorConfig.INHERIT_EPOCH.validate(configuration, (field, value, message) -> problems.add(message));
        assertThat(valid).isFalse();
        assertThat(problems).containsExactly("Inherit epoch cannot be enabled without VitessOrderedTransactionMetadataFactory");
    }

    @Test
    @FixFor("debezium/dbz#2479")
    public void shouldPassInheritEpochValidationWithOrderedTransactionMetadataFactory() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.INHERIT_EPOCH, true)
                .with(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class.getName())
                .build();
        List<String> problems = new ArrayList<>();
        boolean valid = VitessConnectorConfig.INHERIT_EPOCH.validate(configuration, (field, value, message) -> problems.add(message));
        assertThat(valid).isTrue();
        assertThat(problems).isEmpty();
    }

    @Test
    @FixFor("debezium/dbz#2480")
    public void shouldGetGrpcHeadersWithColonsInValue() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.GRPC_HEADERS, "authorization:Bearer a:b:c,x-custom-header:value")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getGrpcHeaders())
                .containsEntry("authorization", "Bearer a:b:c")
                .containsEntry("x-custom-header", "value")
                .hasSize(2);
    }

    @Test
    @FixFor("debezium/dbz#2480")
    public void shouldSkipGrpcHeadersWithoutColon() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.GRPC_HEADERS, "not-a-header,x-custom-header:value")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getGrpcHeaders())
                .containsEntry("x-custom-header", "value")
                .hasSize(1);
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldGetCells() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.CELLS, "cell1,cell2")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getCells()).isEqualTo("cell1,cell2");
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldDefaultCellsToNull() {
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        assertThat(connectorConfig.getCells()).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldExposeCellsInConfigDefinition() {
        assertThat(VitessConnectorConfig.ALL_FIELDS).anyMatch(field -> field.name().equals("vitess.cells"));
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldGetCellPreference() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.CELL_PREFERENCE, "onlyspecified")
                .build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getCellPreference()).isEqualTo(VitessConnectorConfig.CellPreference.ONLY_SPECIFIED);
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldDefaultCellPreferenceToPreferLocalWithAlias() {
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        assertThat(connectorConfig.getCellPreference()).isEqualTo(VitessConnectorConfig.CellPreference.PREFER_LOCAL_WITH_ALIAS);
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldParseCellPreferenceCaseInsensitively() {
        // vtgate parses the value case-insensitively, so mixed case must resolve too.
        assertThat(VitessConnectorConfig.CellPreference.parse("OnlySpecified"))
                .isEqualTo(VitessConnectorConfig.CellPreference.ONLY_SPECIFIED);
        assertThat(VitessConnectorConfig.CellPreference.parse("preferlocalwithalias"))
                .isEqualTo(VitessConnectorConfig.CellPreference.PREFER_LOCAL_WITH_ALIAS);
        assertThat(VitessConnectorConfig.CellPreference.parse("nearest")).isNull();
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldPassCellPreferenceValidationForKnownValues() {
        // vtgate parses the value case-insensitively, so mixed case must validate too.
        for (String value : new String[]{ "preferlocalwithalias", "onlyspecified", "OnlySpecified" }) {
            Configuration configuration = TestHelper.defaultConfig()
                    .with(VitessConnectorConfig.CELL_PREFERENCE, value)
                    .build();
            List<String> problems = new ArrayList<>();
            boolean valid = VitessConnectorConfig.CELL_PREFERENCE.validate(configuration, (field, fieldValue, message) -> problems.add(message));
            assertThat(valid).isTrue();
            assertThat(problems).isEmpty();
        }
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldFailCellPreferenceValidationForUnknownValue() {
        Configuration configuration = TestHelper.defaultConfig()
                .with(VitessConnectorConfig.CELL_PREFERENCE, "nearest")
                .build();
        List<String> problems = new ArrayList<>();
        boolean valid = VitessConnectorConfig.CELL_PREFERENCE.validate(configuration, (field, value, message) -> problems.add(message));
        assertThat(valid).isFalse();
        assertThat(problems).hasSize(1);
    }

    @Test
    @FixFor("debezium/dbz#2547")
    public void shouldExposeCellPreferenceInConfigDefinition() {
        assertThat(VitessConnectorConfig.ALL_FIELDS).anyMatch(field -> field.name().equals("vitess.cell.preference"));
    }

}
