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
    private boolean isInheritEpochEnabled = false;

    public VitessEpochProvider() {
        shardEpochMap = new ShardEpochMap();
    }

    public VitessEpochProvider(ShardEpochMap shardToEpoch, boolean isInheritEpochEnabled) {
        this.shardEpochMap = shardToEpoch;
        this.isInheritEpochEnabled = isInheritEpochEnabled;
    }

    private static boolean isGtidOverridden(String gtid) {
        return gtid.equals(Vgtid.CURRENT_GTID) || gtid.equals(Vgtid.EMPTY_GTID);
    }

    private static boolean isStandardGtid(String gtid) {
        return !isGtidOverridden(gtid);
    }

    public static Long getEpochForGtid(Long previousEpoch, String previousGtidString, String gtidString) {
        if (isGtidOverridden(previousGtidString) && isGtidOverridden(gtidString)) {
            // GTID was overridden, and the current GTID is an overridden value, still waiting for first transaction
            return previousEpoch;
        }
        else if (isGtidOverridden(previousGtidString) && !isGtidOverridden(gtidString)) {
            // GTID was overridden, received first transaction, increment epoch
            LOGGER.info("Incrementing epoch: {}", getLogMessageForGtid(previousEpoch, previousGtidString, gtidString));
            return previousEpoch + 1;
        }
        else if (isStandardGtid(previousGtidString) && isGtidOverridden(gtidString)) {
            // previous GTID is standard, current GTID is overridden, should not be possible, raise exception
            String message = String.format("Current GTID cannot be override value if previous is standard: %s",
                    getLogMessageForGtid(previousEpoch, previousGtidString, gtidString));
            LOGGER.error(message);
            throw new DebeziumException(message);
        }
        else {
            // Both GTIDs are standard so parse them
            return getEpochForStandardGtid(previousEpoch, previousGtidString, gtidString);
        }
    }

    private static Long getEpochForStandardGtid(Long previousEpoch, String previousGtidString, String gtidString) {
        Gtid previousGtid = new Gtid(previousGtidString);
        Gtid gtid = new Gtid(gtidString);
        if (gtid.isHostSetSupersetOf(previousGtid)) {
            return previousEpoch;
        }
        else {
            // Any other case (disjoint set, previous is a superset), VStream has interpreted the previous GTID correctly and sent some new GTID
            // in a continuous stream, so simply increment the epoch
            return previousEpoch + 1;
        }
    }

    private static String getLogMessageForGtid(Long previousEpoch, String previousGtidString, String gtidString) {
        return String.format("GTID: %s, previous GTID: %s, previous Epoch: %s", gtidString, previousGtidString, previousEpoch);
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
        boolean isInheritEpochEnabled = config.getInheritEpoch();
        return new VitessEpochProvider(shardEpochMap, isInheritEpochEnabled);
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
    public void load(Map<String, ?> offsets, VitessConnectorConfig config) {
        String shardToEpochString = (String) offsets.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
        if (!Strings.isNullOrEmpty(shardToEpochString)) {
            shardEpochMap = ShardEpochMap.of(shardToEpochString);
        }
        isInheritEpochEnabled = config.getInheritEpoch();
    }

    public Long getEpoch(String shard, String previousVgtidString, String vgtidString) {
        if (previousVgtidString == null) {
            throw new DebeziumException(String.format("Previous vgtid string cannot be null shard %s current %s", shard, vgtidString));
        }
        try {
            Vgtid vgtid = Vgtid.of(vgtidString);
            Vgtid previousVgtid = Vgtid.of(previousVgtidString);
            this.shardEpochMap = getNewShardEpochMap(previousVgtid, vgtid, shard);
            return shardEpochMap.get(shard);
        }
        catch (Exception e) {
            LOGGER.error("Error providing epoch with shard {}, previousVgtid {}, vgtid {}", shard, previousVgtidString, vgtidString, e);
            throw e;
        }
    }

    private ShardEpochMap getNewShardEpochMap(Vgtid previousVgtid, Vgtid vgtid, String transactionShard) {
        ShardEpochMap newShardEpochMap = new ShardEpochMap();
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
                Long epoch = getEpochForGtid(previousEpoch, previousGtid, gtid);
                newShardEpochMap.put(shard, epoch);
            }
            else {
                // A re-shard happened while we are streaming
                Long epoch;
                if (isInheritEpochEnabled) {
                    epoch = ShardLineage.getInheritedEpoch(shard, shardEpochMap);
                }
                else {
                    epoch = 0L;
                }
                newShardEpochMap.put(shard, epoch);
            }
        }
        return newShardEpochMap;
    }

    public boolean isInheritEpochEnabled() {
        return isInheritEpochEnabled;
    }
}
