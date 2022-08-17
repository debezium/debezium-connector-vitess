/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import io.debezium.connector.common.AbstractPartitionTest;
import io.debezium.util.Collect;

public class VitessPartitionTest extends AbstractPartitionTest<VitessPartition> {

    @Override
    protected VitessPartition createPartition1() {
        return new VitessPartition("server1", null);
    }

    @Override
    protected VitessPartition createPartition2() {
        return new VitessPartition("server2", null);
    }

    @Test
    public void testTaskKeyInPartition() {
        VitessPartition par1 = new VitessPartition("server1", "task1");
        VitessPartition par2 = new VitessPartition("server1", "task2");
        VitessPartition par3 = new VitessPartition("server1", null);
        assertThat(par1).isNotEqualTo(par2);
        assertThat(par1).isNotEqualTo(par3);
        assertThat(par2).isNotEqualTo(par3);
        assertThat(par1.hashCode()).isNotEqualTo(par2.hashCode());
        assertThat(par1.hashCode()).isNotEqualTo(par3.hashCode());
        assertThat(par2.hashCode()).isNotEqualTo(par3.hashCode());
        Map<String, String> parNoTaskKey = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, "server1");
        Map<String, String> parTask1 = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, "server1",
                VitessPartition.TASK_KEY_PARTITION_KEY, "task1");
        Map<String, String> parTask2 = Collect.hashMapOf(VitessPartition.SERVER_PARTITION_KEY, "server1",
                VitessPartition.TASK_KEY_PARTITION_KEY, "task2");
        assertThat(par1.getSourcePartition()).isEqualTo(parTask1);
        assertThat(par2.getSourcePartition()).isEqualTo(parTask2);
        assertThat(par3.getSourcePartition()).isEqualTo(parNoTaskKey);
    }
}
