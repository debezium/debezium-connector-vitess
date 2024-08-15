/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.connector.vitess.TestHelper;

public class ShardEpochMapTest {

    @Test
    public void of() {
        ShardEpochMap epoch = ShardEpochMap.of(TestHelper.TEST_SHARD_TO_EPOCH.toString());
        assertThat(epoch.get(TestHelper.TEST_SHARD1)).isEqualTo(TestHelper.TEST_SHARD1_EPOCH);
    }
}
