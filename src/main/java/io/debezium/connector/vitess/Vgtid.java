/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import binlogdata.Binlogdata;

/** Vitess source position coordinates. */
public class Vgtid {
    public static final String CURRENT_GTID = "current";
    public static final String EMPTY_GTID = "";
    public static final String KEYSPACE_KEY = "keyspace";
    public static final String SHARD_KEY = "shard";
    public static final String GTID_KEY = "gtid";

    public static final String TABLE_P_KS_KEY = "table_p_ks";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @JsonIgnore
    private final Binlogdata.VGtid rawVgtid;
    private final List<ShardGtid> shardGtids;
    private final Map<String, ShardGtid> shardNameToShardGtid;
    // Vgtid cannot be modified, so we store the string representation to improve performance
    private final String vgtidString;

    private Vgtid(Binlogdata.VGtid rawVgtid) {
        this.rawVgtid = rawVgtid;
        List<Vgtid.ShardGtid> shardGtids = new ArrayList<>();
        HashMap<String, ShardGtid> shardNameToShardGtid = new HashMap<>();
        for (Binlogdata.ShardGtid shardGtid : rawVgtid.getShardGtidsList()) {
            TablePrimaryKeys tabletablePrimaryKeys = new TablePrimaryKeys(shardGtid.getTablePKsList());
            ShardGtid currentShardGtid = new ShardGtid(
                    shardGtid.getKeyspace(), shardGtid.getShard(), shardGtid.getGtid(), tabletablePrimaryKeys.getTableLastPrimaryKeys());
            shardGtids.add(currentShardGtid);
            shardNameToShardGtid.put(shardGtid.getShard(), currentShardGtid);
        }
        // This stores a reference to the original, mutable map/list (does not copy contents which would be inefficient)
        this.shardGtids = Collections.unmodifiableList(shardGtids);
        this.shardNameToShardGtid = Collections.unmodifiableMap(shardNameToShardGtid);
        this.vgtidString = shardGtidsToString(shardGtids);
    }

    private Vgtid(List<ShardGtid> shardGtids, String vgtidString) {
        this.vgtidString = vgtidString;
        this.shardGtids = Collections.unmodifiableList(shardGtids);

        HashMap<String, ShardGtid> shardNameToShardGtid = new HashMap<>();
        for (ShardGtid shardGtid : shardGtids) {
            shardNameToShardGtid.put(shardGtid.shard, shardGtid);
        }
        this.shardNameToShardGtid = Collections.unmodifiableMap(shardNameToShardGtid);

        Binlogdata.VGtid.Builder builder = Binlogdata.VGtid.newBuilder();
        for (ShardGtid shardGtid : shardGtids) {
            TablePrimaryKeys tablePrimaryKeys = TablePrimaryKeys.createFromTableLastPrimaryKeys(shardGtid.getTableLastPrimaryKeys());
            builder.addShardGtids(
                    Binlogdata.ShardGtid.newBuilder()
                            .setKeyspace(shardGtid.getKeyspace())
                            .setShard(shardGtid.getShard())
                            .setGtid(shardGtid.getGtid())
                            .addAllTablePKs(tablePrimaryKeys.getRawTableLastPrimaryKeys())
                            .build());
        }
        this.rawVgtid = builder.build();
    }

    public static Vgtid of(String shardGtidsInJson) {
        try {
            List<ShardGtid> shardGtids = MAPPER.readValue(shardGtidsInJson, new TypeReference<List<ShardGtid>>() {
            });
            return of(shardGtids, shardGtidsInJson);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Vgtid of(Binlogdata.VGtid rawVgtid) {
        return new Vgtid(rawVgtid);
    }

    public static Vgtid of(List<ShardGtid> shardGtids, String vgtidString) {
        return new Vgtid(shardGtids, vgtidString);
    }

    public static Vgtid of(List<ShardGtid> shardGtids) {
        return new Vgtid(shardGtids, shardGtidsToString(shardGtids));
    }

    public Binlogdata.VGtid getRawVgtid() {
        return rawVgtid;
    }

    public List<ShardGtid> getShardGtids() {
        return shardGtids;
    }

    public boolean willTriggerVStreamCopy() {
        for (ShardGtid shardGtid : shardGtids) {
            if (shardGtid.getGtid().equals(EMPTY_GTID) || !shardGtid.getTableLastPrimaryKeys().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    public Vgtid getLocalVgtid(String shard) {
        return Vgtid.of(List.of(getShardGtid(shard)));
    }

    public ShardGtid getShardGtid(String shard) {
        ShardGtid shardGtid = shardNameToShardGtid.get(shard);
        return shardGtid;
    }

    public boolean isSingleShard() {
        return rawVgtid.getShardGtidsCount() == 1;
    }

    @Override
    public String toString() {
        return vgtidString;
    }

    private static String shardGtidsToString(List<Vgtid.ShardGtid> shardGtids) {
        try {
            return MAPPER.writeValueAsString(shardGtids);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
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

    @JsonPropertyOrder({ KEYSPACE_KEY, SHARD_KEY, GTID_KEY, TABLE_P_KS_KEY })
    public static class ShardGtid {
        private final String keyspace;
        private final String shard;
        private final String gtid;

        private final List<TablePrimaryKeys.TableLastPrimaryKey> tableLastPrimaryKeys = new ArrayList<>();

        public ShardGtid(@JsonProperty(KEYSPACE_KEY) String keyspace, @JsonProperty(SHARD_KEY) String shard, @JsonProperty(GTID_KEY) String gtid) {
            this(keyspace, shard, gtid, new ArrayList());
        }

        @JsonCreator
        public ShardGtid(@JsonProperty(KEYSPACE_KEY) String keyspace, @JsonProperty(SHARD_KEY) String shard, @JsonProperty(GTID_KEY) String gtid,
                         @JsonProperty(TABLE_P_KS_KEY) List<TablePrimaryKeys.TableLastPrimaryKey> tableLastPrimaryKeys) {
            this.keyspace = keyspace;
            this.shard = shard;
            this.gtid = gtid;
            if (tableLastPrimaryKeys != null) {
                this.tableLastPrimaryKeys.addAll(tableLastPrimaryKeys);
            }
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

        @JsonProperty(TABLE_P_KS_KEY)
        public List<TablePrimaryKeys.TableLastPrimaryKey> getTableLastPrimaryKeys() {
            return tableLastPrimaryKeys;
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
                    Objects.equals(gtid, shardGtid.gtid) &&
                    Objects.equals(tableLastPrimaryKeys, shardGtid.tableLastPrimaryKeys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(keyspace, shard, gtid);
        }
    }
}
