/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TablePrimaryKeysTest.*;
import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_TEMPLATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import io.debezium.util.Collect;

import binlogdata.Binlogdata;

public class VgtidTest {
    public static final String TEST_KEYSPACE = "test_keyspace";
    public static final String TEST_SHARD = "-80";
    public static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    public static final String TEST_SHARD2 = "80-";
    public static final String TEST_GTID2 = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000b:1-1513";

    public static final String VGTID_JSON = String.format(
            VGTID_JSON_TEMPLATE,
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID,
            TEST_KEYSPACE,
            TEST_SHARD2,
            TEST_GTID2);

    public static final String VGTID_JSON_WITH_LAST_PK_TEMPLATE = "[" +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":%s}," +
            "{\"keyspace\":\"%s\",\"shard\":\"%s\",\"gtid\":\"%s\",\"table_p_ks\":%s}" +
            "]";
    public static final String VGTID_JSON_WITH_LAST_PK = String.format(
            VGTID_JSON_WITH_LAST_PK_TEMPLATE,
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID,
            TEST_LAST_PKS_JSON,
            TEST_KEYSPACE,
            TEST_SHARD2,
            TEST_GTID2,
            TEST_LAST_PKS_JSON);

    public static final String VGTID_JSON_WITH_MULTIPLE_TABLE_LAST_PK = String.format(
            VGTID_JSON_WITH_LAST_PK_TEMPLATE,
            TEST_KEYSPACE,
            TEST_SHARD,
            TEST_GTID,
            TEST_MULTIPLE_TABLE_PKS_JSON,
            TEST_KEYSPACE,
            TEST_SHARD2,
            TEST_GTID2,
            TEST_MULTIPLE_TABLE_PKS_JSON);

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
        assertThat(vgtid.getShardGtids()).containsExactly(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2));
        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldCreateFromRawVgtidWithLastPk() {
        Binlogdata.VGtid rawVgtid = Binlogdata.VGtid.newBuilder()
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD)
                        .setGtid(TEST_GTID)
                        .addAllTablePKs(getTestRawTableLastPKList())
                        .build())
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD2)
                        .setGtid(TEST_GTID2)
                        .addAllTablePKs(getTestRawTableLastPKList())
                        .build())
                .build();

        Vgtid vgtid = Vgtid.of(rawVgtid);

        assertThat(vgtid.getRawVgtid()).isEqualTo(rawVgtid);
        assertThat(vgtid.getShardGtids()).containsExactly(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID, getTestTablePKs()),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2, getTestTablePKs()));
        JSONAssert.assertEquals(vgtid.toString(), VGTID_JSON_WITH_LAST_PK, true);
    }

    @Test
    public void shouldCreateFromRawVgtidWithMultipleLastPk() {
        List<Binlogdata.TableLastPK> multipleTablePKs = List.of(getCompPKRawTableLastPK(), getNumericRawTableLastPK());
        Binlogdata.VGtid rawVgtid = Binlogdata.VGtid.newBuilder()
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD)
                        .setGtid(TEST_GTID)
                        .addAllTablePKs(multipleTablePKs)
                        .build())
                .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                        .setKeyspace(TEST_KEYSPACE)
                        .setShard(TEST_SHARD2)
                        .setGtid(TEST_GTID2)
                        .addAllTablePKs(multipleTablePKs)
                        .build())
                .build();

        Vgtid vgtid = Vgtid.of(rawVgtid);

        assertThat(vgtid.getRawVgtid()).isEqualTo(rawVgtid);
        assertThat(vgtid.getShardGtids()).containsExactly(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID, getTestTablePKs(multipleTablePKs)),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2, getTestTablePKs(multipleTablePKs)));
        JSONAssert.assertEquals(vgtid.toString(), VGTID_JSON_WITH_MULTIPLE_TABLE_LAST_PK, true);
    }

    @Test
    public void shouldCreateFromShardGtidsWithLastPk() {
        List<Vgtid.ShardGtid> shardGtids = Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID, getTestTablePKs()),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2, getTestTablePKs()));

        Vgtid vgtid = Vgtid.of(shardGtids);

        assertThat(vgtid.getRawVgtid()).isEqualTo(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD)
                                .setGtid(TEST_GTID)
                                .addAllTablePKs(getTestRawTableLastPKList())
                                .build())
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD2)
                                .setGtid(TEST_GTID2)
                                .addAllTablePKs(getTestRawTableLastPKList())
                                .build())
                        .build());

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID, getTestTablePKs()),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2, getTestTablePKs())));

        JSONAssert.assertEquals(vgtid.toString(), VGTID_JSON_WITH_LAST_PK, true);
    }

    @Test
    public void shouldCreateFromShardGtidsWithLastPkInJson() {
        // exercise SUT
        Vgtid vgtid = Vgtid.of(VGTID_JSON_WITH_LAST_PK);

        // verify outcome
        assertThat(vgtid.getRawVgtid()).isEqualTo(
                Binlogdata.VGtid.newBuilder()
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD)
                                .setGtid(TEST_GTID)
                                .addAllTablePKs(getTestRawTableLastPKList())
                                .build())
                        .addShardGtids(Binlogdata.ShardGtid.newBuilder()
                                .setKeyspace(TEST_KEYSPACE)
                                .setShard(TEST_SHARD2)
                                .setGtid(TEST_GTID2)
                                .addAllTablePKs(getTestRawTableLastPKList())
                                .build())
                        .build());

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID, getTestTablePKs()),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2, getTestTablePKs())));

        JSONAssert.assertEquals(vgtid.toString(), VGTID_JSON_WITH_LAST_PK, true);
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

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2)));

        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldCreateFromShardGtidsInJson() {
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

        assertThat(vgtid.getShardGtids()).isEqualTo(Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2)));

        assertThat(vgtid.toString()).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldEqualsIfEqualityHolds() {
        // setup fixture
        Vgtid vgtid1 = Vgtid.of(VGTID_JSON);
        Vgtid vgtid2 = Vgtid.of(VGTID_JSON);
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
        Vgtid vgtid3 = Vgtid.of(rawVgtid);
        List<Vgtid.ShardGtid> shardGtids = Collect.arrayListOf(
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2));
        Vgtid vgtid4 = Vgtid.of(shardGtids);
        List<Vgtid> vgtids = Collect.arrayListOf(vgtid1, vgtid2, vgtid3, vgtid4);

        // exercise SU
        for (Vgtid vgtid : vgtids) {
            assertThat(vgtids.stream().allMatch(vgtid::equals)).isTrue();
        }
    }
}
