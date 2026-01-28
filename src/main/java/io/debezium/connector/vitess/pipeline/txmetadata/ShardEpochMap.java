/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tracks the mapping of shards to epoch values. Used for serializing/deserializing to JSON and to update
 * epochs per shard.
 */
public class ShardEpochMap {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Map<String, Long> shardEpochMap;

    public ShardEpochMap() {
        this.shardEpochMap = new TreeMap<>();
    }

    public ShardEpochMap(Map<String, Long> shardToEpoch) {
        this.shardEpochMap = new TreeMap<>(shardToEpoch);
    }

    public static ShardEpochMap init(List<String> shards) {
        Map<String, Long> map = shards.stream().collect(Collectors.toMap(s -> s, s -> 0L));
        return new ShardEpochMap(map);
    }

    public static ShardEpochMap of(String shardToEpochJson) {
        try {
            return new ShardEpochMap(MAPPER.readValue(shardToEpochJson, new TypeReference<>() {
            }));
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot read shard epoch map", e);
        }
    }

    @Override
    public String toString() {
        try {
            return MAPPER.writeValueAsString(shardEpochMap);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot convert shard epoch map to string", e);
        }
    }

    public Long get(String shard) {
        return shardEpochMap.get(shard);
    }

    public Set<Map.Entry<String, Long>> entrySet() {
        return shardEpochMap.entrySet();
    }

    public Long put(String shard, Long epoch) {
        return shardEpochMap.put(shard, epoch);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardEpochMap shardToEpoch = (ShardEpochMap) o;
        return Objects.equals(this.shardEpochMap, shardToEpoch.shardEpochMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardEpochMap);
    }

    public Map<String, Long> getMap() {
        return new TreeMap<>(shardEpochMap);
    }
}
