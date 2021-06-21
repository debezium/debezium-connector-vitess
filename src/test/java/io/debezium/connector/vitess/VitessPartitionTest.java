/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

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
}
