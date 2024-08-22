/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import java.util.Map;

/**
 * Class used to determine which parents a shard range descended from. Used to set the epoch to the succeediing
 * epoch of its parents.
 */
public class ShardLineage {

    /**
     * Return the epoch value of the shard, based on its parents epochs.
     * If there are parents present, return the max of the parent epochs plus one.
     * If there are no parents present, it returns zero.
     *
     * @param shardString The descendant shard to find parents of
     * @param shardEpochMap The map to search for parents
     * @return The epoch value of the descendant shard
     */
    public static Long getInheritedEpoch(String shardString, ShardEpochMap shardEpochMap) {
        Shard shard = new Shard(shardString);

        Long maxParentEpoch = -1L;
        for (Map.Entry<String, Long> shardEpoch : shardEpochMap.getMap().entrySet()) {
            String currentShardString = shardEpoch.getKey();
            Long currentEpoch = shardEpoch.getValue();
            Shard currentShard = new Shard(currentShardString);
            if (shard.overlaps(currentShard)) {
                maxParentEpoch = Math.max(maxParentEpoch, currentEpoch);
            }
        }

        return maxParentEpoch + 1;
    }

    private static class Shard {

        // A string lexicographically less than all other strings
        public static final String NEGATIVE_INFINITY = "";
        // A string lexicographically greater than all other strings
        public static final String POSITIVE_INFINITY = "\uFFFF";

        private final String lowerBound;
        private final String upperBound;

        Shard(String shard) {
            String[] shardInterval = getShardInterval(shard.toLowerCase());
            this.lowerBound = getLowerBound(shardInterval);
            this.upperBound = getUpperBound(shardInterval);
            validateBounds();
        }

        private void validateBounds() {
            if (this.lowerBound.compareTo(this.upperBound) >= 0) {
                throw new IllegalArgumentException("Invalid shard range " + this);
            }
        }

        public boolean overlaps(Shard shard) {
            return this.lowerBound.compareTo(shard.upperBound) < 0 && this.upperBound.compareTo(shard.lowerBound) > 0;
        }

        private static String getLowerBound(String[] shardInterval) {
            if (shardInterval.length < 1 || shardInterval[0].isEmpty()) {
                return NEGATIVE_INFINITY;
            }
            return shardInterval[0];
        }

        private static String getUpperBound(String[] shardInterval) {
            if (shardInterval.length != 2 || shardInterval[1].isEmpty()) {
                return POSITIVE_INFINITY;
            }
            return shardInterval[1];
        }

        private static String[] getShardInterval(String shard) {
            return shard.split("-");
        }

        @Override
        public String toString() {
            return "Shard{" +
                    "lowerBound=" + lowerBound +
                    ", upperBound=" + upperBound +
                    "}";
        }
    }
}
