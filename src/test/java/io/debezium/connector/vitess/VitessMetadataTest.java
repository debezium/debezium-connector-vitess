/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.google.protobuf.ByteString;

import io.vitess.proto.Query;

public class VitessMetadataTest {

    @Test
    public void shouldGetNonEmptyShards() {
        List<List<String>> rowValues = List.of(
                List.of("zone1", "keyspace", "-80", "PRIMARY", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace", "-80", "REPLICA", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace", "-80", "RDONLY", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "-80", "PRIMARY", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "-80", "REPLICA", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "-80", "RDONLY", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "80-", "PRIMARY", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "80-", "REPLICA", "SERVING", "zone1-001", "d55", "2024-07-11"),
                List.of("zone1", "keyspace1", "80-", "RDONLY", "SERVING", "zone1-001", "d55", "2024-07-11"));
        List<String> shards = VitessMetadata.getNonEmptyShards(rowValues, "keyspace");
        assertThat(shards).isEqualTo(List.of("-80"));
    }

    @Test
    public void shouldParseRow() {
        List<List<String>> expected = List.of(List.of("foo", "bars"));
        List<Query.Row> rows = List.of(
                Query.Row.newBuilder()
                        .addLengths(3)
                        .addLengths(4)
                        .setValues(ByteString.copyFromUtf8("foobars"))
                        .build());
        assertThat(VitessMetadata.parseRows(rows)).isEqualTo(expected);
    }

    @Test
    public void shouldFlattenAndConcat() {
        List<String> expected = List.of("foo", "bar");
        List<List<String>> input = List.of(List.of("foo"), List.of("bar"));
        assertThat(VitessMetadata.flattenAndConcat(input)).isEqualTo(expected);
    }
}
