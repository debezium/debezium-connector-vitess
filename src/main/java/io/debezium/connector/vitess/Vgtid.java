/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Map;
import java.util.Objects;

import io.debezium.util.Collect;

import binlogdata.Binlogdata;

/** Vitess source position coordiates. */
public class Vgtid {
    public static final String OFFSET_KEYSPACE = "keyspace";
    public static final String OFFSET_SHARD = "shard";
    public static final String OFFSET_GTID = "gtid";

    private final Binlogdata.VGtid rawVgtid;
    private final String keyspace;
    private final String shard;
    private final String gtid;

    private Vgtid(Binlogdata.VGtid rawVgtid) {
        int numOfShardGtids = rawVgtid.getShardGtidsCount();
        if (numOfShardGtids != 1) {
            throw new IllegalArgumentException("Only 1 shard gtid per vgtid is allowed");
        }
        this.rawVgtid = rawVgtid;
        this.keyspace = rawVgtid.getShardGtids(0).getKeyspace();
        this.shard = rawVgtid.getShardGtids(0).getShard();
        this.gtid = rawVgtid.getShardGtids(0).getGtid();
    }

    private Vgtid(String keyspace, String shard, String gtid) {
        this.keyspace = keyspace;
        this.shard = shard;
        this.gtid = gtid;
        this.rawVgtid = Binlogdata.VGtid.newBuilder()
                .addShardGtids(
                        Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(keyspace)
                                .setShard(shard)
                                .setGtid(gtid)
                                .build())
                .build();
    }

    public static Vgtid of(Binlogdata.VGtid rawVgtid) {
        return new Vgtid(rawVgtid);
    }

    public static Vgtid of(String keyspace, String shard, String gtid) {
        return new Vgtid(keyspace, shard, gtid);
    }

    public static Vgtid of(Map<String, Object> offset) {
        if (offset == null || offset.size() != 3) {
            throw new IllegalArgumentException("Offset must have keyspace, shard and gtid");
        }
        return new Vgtid(
                offset.get(OFFSET_KEYSPACE).toString(),
                offset.get(OFFSET_SHARD).toString(),
                offset.get(OFFSET_GTID).toString());
    }

    public Binlogdata.VGtid getRawVgtid() {
        return rawVgtid;
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

    public Map<String, Object> offset() {
        return Collect.hashMapOf(
                OFFSET_KEYSPACE, keyspace,
                OFFSET_SHARD, shard,
                OFFSET_GTID, gtid);
    }

    public Map<String, Object> partition() {
        return Collect.hashMapOf(
                OFFSET_KEYSPACE, keyspace,
                OFFSET_SHARD, shard);
    }

    @Override
    public String toString() {
        return "Vgtid{"
                + "keyspace='"
                + keyspace
                + '\''
                + ", shard='"
                + shard
                + '\''
                + ", gtid='"
                + gtid
                + '\''
                + '}';
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
        return keyspace.equals(vgtid.keyspace) && shard.equals(vgtid.shard) && gtid.equals(vgtid.gtid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyspace, shard, gtid);
    }
}
