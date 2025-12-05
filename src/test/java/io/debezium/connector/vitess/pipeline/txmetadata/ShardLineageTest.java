/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.junit.jupiter.api.Test;

public class ShardLineageTest {

    @Test
    public void shouldGetInheritedEpoch_SingleShard_SplitOneShard() {
        Long parentEpoch = 1L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("0", parentEpoch));
        Long epoch = ShardLineage.getInheritedEpoch("-80", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("80-", shardEpochMap);
        assertThat(epoch2).isEqualTo(parentEpoch + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_IllegalShardRange() {
        Long parentEpoch = 1L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("0", parentEpoch));
        assertThatThrownBy(() -> ShardLineage.getInheritedEpoch("80-30", shardEpochMap))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid shard range");
    }

    @Test
    public void shouldGetInheritedEpoch_MultiShard_SplitOneShard() {
        Long parentEpoch = 1L;
        Long parentEpoch2 = 3L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("-80", parentEpoch, "80-", parentEpoch2));
        Long epoch = ShardLineage.getInheritedEpoch("-40", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("80-c0", shardEpochMap);
        assertThat(epoch2).isEqualTo(parentEpoch2 + 1);
        Long epoch3 = ShardLineage.getInheritedEpoch("c0-", shardEpochMap);
        assertThat(epoch3).isEqualTo(parentEpoch2 + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_MultiShard_SplitOneShard_UpperEqualsLowerBound() {
        Long parentEpoch = 3L;
        Long parentEpoch2 = 1L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("-80", parentEpoch, "80-", parentEpoch2));
        // The lower bound (80) is equal to a previous shard's upper bound (-80)
        Long epoch2 = ShardLineage.getInheritedEpoch("80-c0", shardEpochMap);
        // This is not a descendant so assert the epoch is from the actual parent (which is less than the other parent)
        assertThat(epoch2).isEqualTo(parentEpoch2 + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_TwoToFourShards() {
        Long parentEpoch = 1L;
        Long parentEpoch2 = 3L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("-80", parentEpoch, "80-", parentEpoch2));
        Long epoch = ShardLineage.getInheritedEpoch("-4000", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("8000-c000", shardEpochMap);
        assertThat(epoch2).isEqualTo(parentEpoch2 + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_FourShards() {
        Long parentEpoch1 = 1L;
        Long parentEpoch2 = 2L;
        Long parentEpoch3 = 3L;
        Long parentEpoch4 = 4L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("-4000", parentEpoch1,
                "4000-8000", parentEpoch2, "8000-c000", parentEpoch3, "c000-", parentEpoch4));
        Long epoch = ShardLineage.getInheritedEpoch("4000-6000", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch2 + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("5000-9000", shardEpochMap);
        assertThat(epoch2).isEqualTo(Math.max(parentEpoch2, parentEpoch3) + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_OneToTwoShardsHigherHexRange() {
        Long parentEpoch1 = 1L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("b7-b8", parentEpoch1));
        Long epoch = ShardLineage.getInheritedEpoch("b720-b750", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch1 + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("b750-b820", shardEpochMap);
        assertThat(epoch2).isEqualTo(parentEpoch1 + 1);
    }

    @Test
    public void shouldGetInheritedEpoch_OneToTwoShardsMixedCase() {
        Long parentEpoch1 = 4L;
        ShardEpochMap shardEpochMap = new ShardEpochMap(Map.of("B7-B8", parentEpoch1));
        Long epoch = ShardLineage.getInheritedEpoch("b720-b750", shardEpochMap);
        assertThat(epoch).isEqualTo(parentEpoch1 + 1);
        Long epoch2 = ShardLineage.getInheritedEpoch("b750-b820", shardEpochMap);
        assertThat(epoch2).isEqualTo(parentEpoch1 + 1);

        ShardEpochMap shardEpochMap2 = new ShardEpochMap(Map.of("b7-b8", parentEpoch1));
        Long epoch3 = ShardLineage.getInheritedEpoch("B720-B750", shardEpochMap);
        assertThat(epoch3).isEqualTo(parentEpoch1 + 1);
        Long epoch4 = ShardLineage.getInheritedEpoch("B750-B820", shardEpochMap);
        assertThat(epoch4).isEqualTo(parentEpoch1 + 1);
    }
}
