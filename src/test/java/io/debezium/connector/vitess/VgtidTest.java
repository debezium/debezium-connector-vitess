/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.junit.Test;

import io.debezium.util.Collect;

import binlogdata.Binlogdata;

public class VgtidTest {

    @Test
    public void shouldReturnOffset() {
        Binlogdata.ShardGtid shardGtid = Binlogdata.ShardGtid.newBuilder()
                .setKeyspace("foo")
                .setShard("0")
                .setGtid("MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-2")
                .build();

        Binlogdata.VGtid vgtid = Binlogdata.VGtid.newBuilder().addShardGtids(shardGtid).build();

        Map<String, Object> offset = Vgtid.of(vgtid).offset();
        assertThat(offset).hasSize(3);
        assertThat(offset.get(Vgtid.OFFSET_KEYSPACE)).isEqualTo("foo");
        assertThat(offset.get(Vgtid.OFFSET_SHARD)).isEqualTo("0");
        assertThat(offset.get(Vgtid.OFFSET_GTID))
                .isEqualTo("MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-2");
    }

    @Test
    public void shouldCreateFromOffset() {
        Map<String, Object> expected = Collect.hashMapOf(
                Vgtid.OFFSET_KEYSPACE, "foo",
                Vgtid.OFFSET_SHARD, "0",
                Vgtid.OFFSET_GTID, "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-2");

        assertThat(Vgtid.of(expected).offset()).isEqualTo(expected);
    }
}
