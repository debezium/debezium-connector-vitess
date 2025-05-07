/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.TEST_SHARD_TO_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.schema.DefaultTopicNamingStrategy;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessConnectorConfigTest {

    @Test
    public void shouldGetVitessHeartbeatImplWhenIntervalSet() {
        Configuration configuration = TestHelper.defaultConfig().with(
                Heartbeat.HEARTBEAT_INTERVAL, 1000).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        Heartbeat heartbeat = connectorConfig.createHeartbeat(
                DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.NO_OP,
                null,
                null);
        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat instanceof VitessHeartbeatImpl).isTrue();
    }

    @Test
    public void shouldGetVitessHeartbeatNoOp() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        Heartbeat heartbeat = connectorConfig.createHeartbeat(
                DefaultTopicNamingStrategy.create(connectorConfig),
                SchemaNameAdjuster.NO_OP,
                null,
                null);
        assertThat(heartbeat).isNotNull();
        assertThat(heartbeat).isEqualTo(Heartbeat.DEFAULT_NOOP_HEARTBEAT);
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
    public void shouldDefaultDisableStreamKeyspaceHeartbeatsConfig() {
        Configuration configuration = TestHelper.defaultConfig().build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(connectorConfig.getStreamKeyspaceHeartbeats()).isFalse();
    }

}
