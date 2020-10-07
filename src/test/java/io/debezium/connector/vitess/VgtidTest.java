/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import io.debezium.util.Collect;

import binlogdata.Binlogdata;

public class VgtidTest {
    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_SHARD = "-80";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    private static final String TEST_SHARD2 = "80-";
    private static final String TEST_GTID2 = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000b:1-1513";
    private static final String VGTID_JSON = String.format(
            "[" +
                    "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}," +
                    "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\"}" +
                    "]",
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID,
            TEST_KEYSPACE,
            TEST_SHARD2,
            TEST_GTID2);

    @Test
    public void shouldCreateFromRawVgtid() {
        // setup fixture
        Binlogdata.VGtid rawVgtid = Binlogdata.VGtid.newBuilder()
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD)
                        .setGtid(TEST_GTID)
                        .build())
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD2)
                        .setGtid(TEST_GTID2)
                        .build())
                .build();

        // exercise SUT
        Vgtid vgtid = Vgtid.of(rawVgtid);

        // verify outcome
        assertThat(vgtid.getRawVgtid()).isEqualTo(rawVgtid);
        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.unmodifiableSet(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2)));
        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldCreateFromShardGtids() {
        // setup fixture
        List<Vgtid.ShardGtid> shardGtids = Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2));

        // exercise SUT
        Vgtid vgtid = Vgtid.of(shardGtids);

        // verify outcome
        assertThat(vgtid.getRawVgtid()).isEqualTo(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD)
                                .setGtid(TEST_GTID)
                                .build())
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD2)
                                .setGtid(TEST_GTID2)
                                .build())
                        .build());

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.unmodifiableSet(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2)));

        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldCreateFromShardGtidsInJson() {
        // setup fixture
        List<Vgtid.ShardGtid> shardGtids = Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2));

        // exercise SUT
        Vgtid vgtid = Vgtid.of(VGTID_JSON);

        // verify outcome
        assertThat(vgtid.getRawVgtid()).isEqualTo(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD)
                                .setGtid(TEST_GTID)
                                .build())
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD2)
                                .setGtid(TEST_GTID2)
                                .build())
                        .build());

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.unmodifiableSet(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2)));

        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }
}
