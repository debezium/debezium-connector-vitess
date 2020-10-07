/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import io.debezium.util.Collect;

public class VitessOffsetContextTest {

    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_SHARD = "-80";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    private static final String TEST_SHARD2 = "80-";
    private static final String TEST_GTID2 = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000b:1-1513";
    private static final Long TEST_EVENTS_TO_SKIP = 3L;
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

    private VitessOffsetContext.Loader loader;
    private VitessOffsetContext offsetContext;

    @Before
    public void before() {
        loader = new VitessOffsetContext.Loader(
                new VitessConnectorConfig(TestHelper.defaultConfig().build()));

        offsetContext = (VitessOffsetContext) loader.load(
                Collect.hashMapOf(
                        SourceInfo.VGTID,
                        VGTID_JSON,
                        SourceInfo.EVENTS_TO_SKIP,
                        TEST_EVENTS_TO_SKIP));
    }

    @Test
    public void shouldBeAbleToLoadFromOffset() {
        // verify outcome
        Assertions.assertThat(offsetContext).isNotNull();
        assertThat(offsetContext.isSkipEvent()).isEqualTo(true);
        assertThat(offsetContext.getInitialEventsToSkip()).isEqualTo(TEST_EVENTS_TO_SKIP);
        assertThat(offsetContext.getRestartEventsToSkip()).isEqualTo(0);
        assertThat(offsetContext.getPartition()).isNotNull();
        assertThat(offsetContext.getRestartVgtid()).isEqualTo(Vgtid.of(
                Collect.arrayListOf(
                        new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                        new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2))));
        assertThat((offsetContext).getTransactionContext()).isNotNull();
    }

    @Test
    public void shouldRotateToNewVGgtid() {
        // exercise SUT
        offsetContext.rotateVgtid(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, "new_gtid"),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, "new_gtid"))),
                AnonymousValue.getInstant());

        // verify outcome
        assertThat(offsetContext.getRestartVgtid()).isEqualTo(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2))));
        assertThat(offsetContext.getInitialEventsToSkip()).isEqualTo(TEST_EVENTS_TO_SKIP);
        assertThat(offsetContext.getRestartEventsToSkip()).isEqualTo(0L);

        // exercise SUT
        for (int i = 0; i < TEST_EVENTS_TO_SKIP; i++) {
            offsetContext.startRowEvent(AnonymousValue.getInstant(), null);
        }

        // verify outcome
        assertThat(offsetContext.getInitialEventsToSkip()).isEqualTo(0);
        assertThat(offsetContext.isSkipEvent()).isEqualTo(false);
        assertThat(offsetContext.getRestartEventsToSkip()).isEqualTo(TEST_EVENTS_TO_SKIP);
    }

    @Test
    public void shouldBeAbleToConvertToOffset() {
        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).isNotNull();
        assertThat(offset.get(SourceInfo.VGTID)).isEqualTo(VGTID_JSON);
        // for each row event, we increment event-to-skip by 1
        assertThat(offset.get(SourceInfo.EVENTS_TO_SKIP)).isEqualTo(0L);
    }
}
