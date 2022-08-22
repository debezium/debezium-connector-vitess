/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.common.AbstractPartitionTest;

public class VitessPartitionTest extends AbstractPartitionTest<VitessPartition> {

    @Override
    protected VitessPartition createPartition1() {
        return new VitessPartition("server1");
    }

    @Override
    protected VitessPartition createPartition2() {
        return new VitessPartition("server2");
    }

    @Test
    public void sameShardsShouldBeEqual() {
        VitessPartition partition1 = new VitessPartition("server1", "shard1");
        VitessPartition partition2 = new VitessPartition("server1", "shard1");
        assertThat(partition1).isEqualTo(partition2);
        assertThat(partition1.hashCode()).isEqualTo(partition2.hashCode());
    }

    @Test
    public void differentShardsShouldNotBeEqual() {
        VitessPartition partition1 = new VitessPartition("server1", "shard1");
        VitessPartition partition2 = new VitessPartition("server1", "shard2");
        assertThat(partition1).isNotEqualTo(partition2);
        assertThat(partition1.hashCode()).isNotEqualTo(partition2.hashCode());
    }

    @Test
    public void nullShardShouldNotBeEqualToShard() {
        VitessPartition partition1 = new VitessPartition("server1");
        VitessPartition partition2 = new VitessPartition("server1", "shard1");
        assertThat(partition1).isNotEqualTo(partition2);
        assertThat(partition1.hashCode()).isNotEqualTo(partition2.hashCode());
    }
}
