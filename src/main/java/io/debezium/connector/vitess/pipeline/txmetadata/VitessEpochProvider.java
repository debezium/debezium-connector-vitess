/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.DebeziumException;
import io.debezium.connector.vitess.Vgtid;

public class VitessEpochProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessEpochProvider.class);
    private Map<String, Long> shardToEpoch = new HashMap<>();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static boolean isInvalidGtid(String gtid) {
        return gtid.equals(Vgtid.CURRENT_GTID) || gtid.equals(Vgtid.EMPTY_GTID);
    }

    public static Long getEpochForGtid(Long previousEpoch, String previousGtidString, String gtidString) {
        if (isInvalidGtid(previousGtidString)) {
            return previousEpoch + 1;
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

    public Map<String, Object> store(Map<String, Object> offset) {
        try {
            offset.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, MAPPER.writeValueAsString(shardToEpoch));
            return offset;
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot store epoch: " + shardToEpoch.toString());
        }
    }

    public void load(Map<String, ?> offsets) {
        try {
            String shardToEpochString = (String) offsets.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH);
            if (shardToEpochString != null) {
                shardToEpoch = MAPPER.readValue(shardToEpochString, new TypeReference<Map<String, Long>>() {
                });
            }
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Cannot read shardToEpoch from offsets: " + offsets);
        }
    }

    public Long getEpoch(String shard, String previousVgtidString, String vgtidString) {
        if (previousVgtidString == null) {
            if (shardToEpoch.get(shard) != null) {
                throw new DebeziumException("Previous VGTID is null but shardToEpoch map is not null: " + shardToEpoch.toString() +
                        ", update VGTID in offsets to resume");
            }
            // When the connector is first created it has no previous VGTID in offsets (and there is no epoch stored)
            long epoch = 0L;
            storeEpoch(shard, epoch);
            return epoch;
        }

        Vgtid vgtid = Vgtid.of(vgtidString);
        Vgtid previousVgtid = Vgtid.of(previousVgtidString);
        String previousGtid = previousVgtid.getShardGtid(shard).getGtid();
        String gtid = vgtid.getShardGtid(shard).getGtid();
        long previousEpoch = shardToEpoch.getOrDefault(shard, 0L);
        long currentEpoch = getEpochForGtid(previousEpoch, previousGtid, gtid);
        storeEpoch(shard, currentEpoch);
        return currentEpoch;
    }

    private void storeEpoch(String shard, long epoch) {
        shardToEpoch.put(shard, epoch);
    }
}
