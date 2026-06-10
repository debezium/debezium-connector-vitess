/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.protobuf.ByteString;

import io.vitess.proto.Query;

public class VitessMetadataTest {

    @Test
    public void shouldInsertWorkloadNameInAllQueries() {
        String expectedQuery = "/*vt+ WORKLOAD_NAME=debezium */ Select * keyspace.foo;";
        String actualQuery = VitessMetadata.formatQuery("Select * %s.foo;", "keyspace");
        assertThat(actualQuery).isEqualTo(expectedQuery);
    }

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

    @Test
    public void shouldQuoteIdentifier() {
        assertThat(VitessMetadata.quoteIdentifier("keyspace")).isEqualTo("`keyspace`");
        assertThat(VitessMetadata.quoteIdentifier("tenant-a")).isEqualTo("`tenant-a`");
        assertThat(VitessMetadata.quoteIdentifier("weird`name")).isEqualTo("`weird``name`");
    }

    @Test
    public void shouldEscapeStringLiteral() {
        assertThat(VitessMetadata.escapeStringLiteral("plain")).isEqualTo("plain");
        assertThat(VitessMetadata.escapeStringLiteral("it's")).isEqualTo("it\\'s");
        assertThat(VitessMetadata.escapeStringLiteral("back\\slash")).isEqualTo("back\\\\slash");
    }

    @Test
    public void shouldEscapeLikePattern() {
        assertThat(VitessMetadata.escapeLikePattern("plain")).isEqualTo("plain");
        assertThat(VitessMetadata.escapeLikePattern("foo_bar")).isEqualTo("foo\\_bar");
        assertThat(VitessMetadata.escapeLikePattern("100%")).isEqualTo("100\\%");
        // Backslash must be escaped first so the backslashes added for `_` / `%` are not themselves doubled.
        assertThat(VitessMetadata.escapeLikePattern("a\\b_c")).isEqualTo("a\\\\b\\_c");
    }
}
