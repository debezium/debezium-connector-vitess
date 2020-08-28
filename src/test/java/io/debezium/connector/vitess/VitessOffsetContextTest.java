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
    private static final String TEST_SHARD = "0";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    private static final Long TEST_EVENTS_TO_SKIP = 3L;

    private VitessOffsetContext.Loader loader;
    private VitessOffsetContext offsetContext;

    @Before
    public void before() {
        loader = new VitessOffsetContext.Loader(
                new VitessConnectorConfig(TestHelper.defaultConfig().build()));

        offsetContext = (VitessOffsetContext) loader.load(
                Collect.hashMapOf(
                        SourceInfo.VGTID_KEYSPACE,
                        TEST_KEYSPACE,
                        SourceInfo.VGTID_SHARD,
                        TEST_SHARD,
                        SourceInfo.VGTID_GTID,
                        TEST_GTID,
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
        assertThat(offsetContext.getRestartVgtid())
                .isEqualTo(Vgtid.of(TEST_KEYSPACE, TEST_SHARD, TEST_GTID));
        assertThat((offsetContext).getTransactionContext()).isNotNull();
    }

    @Test
    public void shouldRotateToNewVGgtid() {
        // exercise SUT
        offsetContext.rotateVgtid(
                Vgtid.of(TEST_KEYSPACE, TEST_SHARD, "new_gtid"), AnonymousValue.getInstant());

        // verify outcome
        assertThat(offsetContext.getRestartVgtid())
                .isEqualTo(Vgtid.of(TEST_KEYSPACE, TEST_SHARD, TEST_GTID));
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
        assertThat(offset.get(SourceInfo.VGTID_KEYSPACE)).isEqualTo(TEST_KEYSPACE);
        assertThat(offset.get(SourceInfo.VGTID_SHARD)).isEqualTo(TEST_SHARD);
        assertThat(offset.get(SourceInfo.VGTID_GTID)).isEqualTo(TEST_GTID);
        // for each row event, we increment event-to-skip by 1
        assertThat(offset.get(SourceInfo.EVENTS_TO_SKIP)).isEqualTo(0L);
    }
}
