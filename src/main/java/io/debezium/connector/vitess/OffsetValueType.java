/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.connector.vitess.pipeline.txmetadata.ShardEpochMap;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;

public enum OffsetValueType {

    GTID(SourceInfo.VGTID_KEY, OffsetValueType::parseGtid,
            OffsetValueType::getVgtid, OffsetValueType::getConfigGtidsPerShard),
    EPOCH(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, OffsetValueType::parseEpoch,
            OffsetValueType::getShardEpochMap, OffsetValueType::getConfigShardEpochMapPerShard);

    public final String name;
    public final Function<String, Map<String, Object>> parserFunction;
    public final BiFunction<Map<String, Object>, String, Object> conversionFunction;
    public final BiFunction<VitessConnectorConfig, List<String>, Map<String, Object>> configValuesFunction;

    OffsetValueType(String typeName, Function<String, Map<String, Object>> parserFunction,
                    BiFunction<Map<String, Object>, String, Object> conversionFunction,
                    BiFunction<VitessConnectorConfig, List<String>, Map<String, Object>> configValuesFunction) {
        this.name = typeName;
        this.parserFunction = parserFunction;
        this.conversionFunction = conversionFunction;
        this.configValuesFunction = configValuesFunction;
    }

    private static Map<String, Object> parseGtid(String vgtidStr) {
        Map<String, Object> shardToGtid = new HashMap<>();
        List<Vgtid.ShardGtid> shardGtids = Vgtid.of(vgtidStr).getShardGtids();
        for (Vgtid.ShardGtid shardGtid : shardGtids) {
            shardToGtid.put(shardGtid.getShard(), shardGtid.getGtid());
        }
        return shardToGtid;
    }

    private static Map<String, Object> parseEpoch(String epochString) {
        ShardEpochMap shardToEpoch = ShardEpochMap.of(epochString);
        return (Map) shardToEpoch.getMap();
    }

    /**
     * Get the {@link ShardEpochMap} from this map of shards to epochs.
     *
     * @param epochMap Map of shards to epoch values
     * @param keyspace Needed to match the function signature of getVgtid, ignored
     * @return The {@link ShardEpochMap}
     */
    static ShardEpochMap getShardEpochMap(Map<String, ?> epochMap, String keyspace) {
        return new ShardEpochMap((Map<String, Long>) epochMap);
    }

    static Vgtid getVgtid(Map<String, ?> gtidsPerShard, String keyspace) {
        List<Vgtid.ShardGtid> shardGtids = new ArrayList();
        for (Map.Entry<String, ?> entry : gtidsPerShard.entrySet()) {
            shardGtids.add(new Vgtid.ShardGtid(keyspace, entry.getKey(), (String) entry.getValue()));
        }
        return Vgtid.of(shardGtids);
    }

    static Map<String, Object> getConfigShardEpochMapPerShard(VitessConnectorConfig connectorConfig, List<String> shards) {
        String shardEpochMapString = connectorConfig.getShardEpochMap();
        Function<Integer, Long> initEpoch = x -> 0L;
        Map<String, Long> shardEpochMap;
        if (shardEpochMapString.isEmpty()) {
            shardEpochMap = buildMap(shards, initEpoch);
        }
        else {
            shardEpochMap = ShardEpochMap.of(shardEpochMapString).getMap();
        }
        return (Map) shardEpochMap;
    }

    static Map<String, Object> getConfigGtidsPerShard(VitessConnectorConfig connectorConfig, List<String> shards) {
        String gtids = connectorConfig.getVgtid();
        Map<String, String> configGtidsPerShard = null;
        if (shards != null && gtids.equals(Vgtid.EMPTY_GTID)) {
            Function<Integer, String> emptyGtid = x -> Vgtid.EMPTY_GTID;
            configGtidsPerShard = buildMap(shards, emptyGtid);
        }
        else if (shards != null && gtids.equals(Vgtid.CURRENT_GTID)) {
            Function<Integer, String> currentGtid = x -> Vgtid.CURRENT_GTID;
            configGtidsPerShard = buildMap(shards, currentGtid);
        }
        else if (shards != null) {
            List<Vgtid.ShardGtid> shardGtids = Vgtid.of(gtids).getShardGtids();
            Map<String, String> shardsToGtid = new HashMap<>();
            for (Vgtid.ShardGtid shardGtid : shardGtids) {
                shardsToGtid.put(shardGtid.getShard(), shardGtid.getGtid());
            }
            Function<Integer, String> shardGtid = (i -> shardsToGtid.get(shards.get(i)));
            configGtidsPerShard = buildMap(shards, shardGtid);
        }
        return (Map) configGtidsPerShard;
    }

    private static <T> Map<String, T> buildMap(List<String> keys, Function<Integer, T> function) {
        return IntStream.range(0, keys.size())
                .boxed()
                .collect(Collectors.toMap(keys::get, function));
    }
}
