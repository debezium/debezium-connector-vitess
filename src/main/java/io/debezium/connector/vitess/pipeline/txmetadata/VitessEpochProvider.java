/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessConnectorTask;
import io.debezium.connector.vitess.connection.VitessReplicationConnection;
import io.debezium.util.Strings;

public class VitessEpochProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessEpochProvider.class);
    private ShardEpochMap shardEpochMap;
    private boolean isFirstTransaction = true;

    public VitessEpochProvider() {
        shardEpochMap = new ShardEpochMap();
    }

    public VitessEpochProvider(ShardEpochMap shardToEpoch) {
        this.shardEpochMap = shardToEpoch;
    }

    private static boolean isInvalidGtid(String gtid) {
        return gtid.equals(Vgtid.CURRENT_GTID) || gtid.equals(Vgtid.EMPTY_GTID);
    }

    public static Long getEpochForGtid(Long previousEpoch, String previousGtidString, String gtidString, boolean isFirstTransaction) {
        if (isFirstTransaction && isInvalidGtid(previousGtidString)) {
            return previousEpoch + 1;
        }
        else if (isInvalidGtid(previousGtidString)) {
            throw new DebeziumException("Invalid GTID: The previous GTID cannot be one of current or empty after the first transaction " + gtidString);
        }
        if (isInvalidGtid(gtidString)) {
            throw new DebeziumException("Invalid GTID: The current GTID cannot be one of current or empty " + gtidString);
        }
        Gtid previousGtid = new Gtid(previousGtidString);
        Gtid gtid = new Gtid(gtidString);
        if (previousGtid.isHostSetEqual(gtid) || gtid.isHostSetSupersetOf(previousGtid)) {
            return previousEpoch;
        }
        else if (gtid.isHostSetSubsetOf(previousGtid)) {
            return previousEpoch + 1;
        }
        else {
            LOGGER.error(
                    "Error determining epoch, previous host set: {}, host set: {}",
                    previousGtid, gtid);
            throw new RuntimeException("Can't determine epoch");
        }
    }

    public ShardEpochMap getShardEpochMap() {
        return shardEpochMap;
    }

    /**
     * Initialize the VitessEpochProvider. Called if either:
     * 1. Change in offset storage generation (task number change or vitess shard set change): Read from the config that is set to be the
     * shard epoch map derived from previous generation and other info in {@link VitessConnectorTask}
     * 2. Newly created connector: Set all shards equal to 0 to initialize shardToEpoch map
     *
     * @param config VitessConnectorConfig to use for initialization
     * @return VitessEpochProvider
     */
    public static VitessEpochProvider initialize(VitessConnectorConfig config) {
        ShardEpochMap shardEpochMap = VitessReplicationConnection.defaultShardEpochMap(config);
        return new VitessEpochProvider(shardEpochMap);
    }

    public Map<String, Object> store(Map<String, Object> offset) {
        offset.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, shardEpochMap.toString());
        return offset;
    }

    /**
     * Load the shard epoch map from offsets. If we enabled ordered transaction metadata for the first time,
     * then there will be no offsets so use default empty map
     *
     * @param offsets Offsets to load
     */
    public void load(Map<String, ?> offsets) {
        String shardToEpochString = (String) offsets.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
        if (!Strings.isNullOrEmpty(shardToEpochString)) {
            shardEpochMap = ShardEpochMap.of(shardToEpochString);
        }
    }

    public Long getEpoch(String shard, String previousVgtidString, String vgtidString) {
        if (previousVgtidString == null) {
            throw new DebeziumException(String.format("Previous vgtid string cannot be null shard %s current %s", shard, vgtidString));
        }
        Vgtid vgtid = Vgtid.of(vgtidString);
        Vgtid previousVgtid = Vgtid.of(previousVgtidString);
        processVgtid(previousVgtid, vgtid);
        if (isFirstTransaction) {
            isFirstTransaction = false;
        }
        return shardEpochMap.get(shard);
    }

    private void processVgtid(Vgtid previousVgtid, Vgtid vgtid) {
        for (Vgtid.ShardGtid shardGtid : vgtid.getShardGtids()) {
            String shard = shardGtid.getShard();
            String gtid = shardGtid.getGtid();
            Vgtid.ShardGtid previousShardGtid = previousVgtid.getShardGtid(shard);
            if (previousShardGtid != null) {
                String previousGtid = previousShardGtid.getGtid();
                // If there is a previous GTID, then we should have initialized shardEpochMap with the shard
                Long previousEpoch = shardEpochMap.get(shard);
                if (previousEpoch == null) {
                    throw new DebeziumException(String.format(
                            "Previous epoch cannot be null for shard %s when shard present in previous vgtid %s",
                            shard, previousVgtid));
                }
                Long epoch = getEpochForGtid(previousEpoch, previousGtid, gtid, isFirstTransaction);
                shardEpochMap.put(shard, epoch);
            }
            else {
                // A re-shard happened while we are streaming set the new value to zero
                // TODO: Add support to inherit epoch from ancestor shard
                shardEpochMap.put(shard, 0L);
            }
        }
        // Note: we could purge all shards from the shard epoch map that are not present in the current vgtid.
        // However, this poses some risk of losing epoch values, so we leave them as is. There may be dormant shards
        // that we still have epoch values for, but that should be fine. Once we allow for epochs to be inherited from other shards
        // we could reconsider purging them to ensure the epoch shard map does not grow too large.
    }
}
