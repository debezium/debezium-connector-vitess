/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;

import binlogdata.Binlogdata;

/** Vitess source position coordinates. */
public class Vgtid {
    public static final String CURRENT_GTID = "current";
    public static final String KEYSPACE_KEY = "keyspace";
    public static final String SHARD_KEY = "shard";
    public static final String GTID_KEY = "gtid";

    private static final Gson gson = new GsonBuilder().registerTypeAdapter(ShardGtid.class, new ShardGtidSerializer()).create();

    private final Binlogdata.VGtid rawVgtid;
    private final List<ShardGtid> shardGtids = new ArrayList<>();

    private Vgtid(Binlogdata.VGtid rawVgtid) {
        this.rawVgtid = rawVgtid;
        for (Binlogdata.ShardGtid shardGtid : rawVgtid.getShardGtidsList()) {
            shardGtids.add(new ShardGtid(shardGtid.getKeyspace(), shardGtid.getShard(), shardGtid.getGtid()));
        }
    }

    private Vgtid(List<ShardGtid> shardGtids) {
        this.shardGtids.addAll(shardGtids);

        Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
        for (ShardGtid shardGtid : shardGtids) {
            builder.addShardGtids(
                    Binlogdata.ShardGtid.newBuilder()
                            .setKeyspace(shardGtid.getKeyspace())
                            .setShard(shardGtid.getShard())
                            .setGtid(shardGtid.getGtid())
                            .build());
        }
        this.rawVgtid = builder.build();
    }

    public static Vgtid of(String shardGtidsInJson) {
        List<Vgtid.ShardGtid> shardGtids = gson.fromJson(shardGtidsInJson, new TypeToken<List<ShardGtid>>() {
        }.getType());
        return of(shardGtids);
    }

    public static Vgtid of(Binlogdata.VGtid rawVgtid) {
        return new Vgtid(rawVgtid);
    }

    public static Vgtid of(List<ShardGtid> shardGtids) {
        return new Vgtid(shardGtids);
    }

    public Binlogdata.VGtid getRawVgtid() {
        return rawVgtid;
    }

    public List<ShardGtid> getShardGtids() {
        return shardGtids;
    }

    public boolean isSingleShard() {
        return rawVgtid.getShardGtidsCount() == 1;
    }

    @Override
    public String toString() {
        return gson.toJson(shardGtids);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Vgtid vgtid = (Vgtid) o;
        return Objects.equals(rawVgtid, vgtid.rawVgtid) &&
                Objects.equals(shardGtids, vgtid.shardGtids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawVgtid, shardGtids);
    }

    public static class ShardGtid {
        private final String keyspace;
        private final String shard;
        private final String gtid;

        public ShardGtid(String keyspace, String shard, String gtid) {
            this.keyspace = keyspace;
            this.shard = shard;
            this.gtid = gtid;
        }

        public String getKeyspace() {
            return keyspace;
        }

        public String getShard() {
            return shard;
        }

        public String getGtid() {
            return gtid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ShardGtid shardGtid = (ShardGtid) o;
            return Objects.equals(keyspace, shardGtid.keyspace) &&
                    Objects.equals(shard, shardGtid.shard) &&
                    Objects.equals(gtid, shardGtid.gtid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyspace, shard, gtid);
        }
    }

    /**
     * Ensure keys in JSON object are in expected order during serialization.
     */
    private static class ShardGtidSerializer implements JsonSerializer<ShardGtid> {
        @Override
        public JsonElement serialize(ShardGtid obj, Type type, JsonSerializationContext context) {
            JsonObject json = new JsonObject();
            json.add(KEYSPACE_KEY, context.serialize(obj.getKeyspace()));
            json.add(SHARD_KEY, context.serialize(obj.getShard()));
            json.add(GTID_KEY, context.serialize(obj.getGtid()));
            return json;
        }
    }
}
