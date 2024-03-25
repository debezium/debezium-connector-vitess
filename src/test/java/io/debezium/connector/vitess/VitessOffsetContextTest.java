/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.debezium.connector.vitess.TestHelper.VGTID_JSON_TEMPLATE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.vitess.transaction.OrderedTransactionContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.util.Clock;
import io.debezium.util.Collect;

public class VitessOffsetContextTest {

    private static final String TEST_KEYSPACE = "test_keyspace";
    private static final String TEST_SHARD = "-80";
    private static final String TEST_GTID = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000a:1-1513";
    private static final String TEST_SHARD2 = "80-";
    private static final String TEST_GTID2 = "MySQL56/a790d864-9ba1-11ea-99f6-0242ac11000b:1-1513";
    private static final String VGTID_JSON = String.format(
            VGTID_JSON_TEMPLATE,
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

        offsetContext = (VitessOffsetContext) loader.load(Collect.hashMapOf(SourceInfo.VGTID_KEY, VGTID_JSON));
    }

    @Test
    public void shouldBeAbleToLoadFromOffset() {
        // verify outcome
        assertThat(offsetContext).isNotNull();
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
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, "new_gtid2"))),
                AnonymousValue.getInstant());

        // verify outcome
        assertThat(offsetContext.getRestartVgtid()).isEqualTo(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, TEST_GTID),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, TEST_GTID2))));
    }

    @Test
    public void shouldBeAbleToConvertToOffset() {
        Map<String, ?> offset = offsetContext.getOffset();
        assertThat(offset).isNotNull();
        assertThat(offset.get(SourceInfo.VGTID_KEY)).isEqualTo(VGTID_JSON);
    }

    @Test
    public void shouldResetToNewVGgtid() {
        // exercise SUT
        offsetContext.resetVgtid(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, "new_gtid"),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, "new_gtid2"))),
                AnonymousValue.getInstant());

        // verify outcome
        assertThat(offsetContext.getRestartVgtid()).isEqualTo(
                Vgtid.of(
                        Collect.arrayListOf(
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD, "new_gtid"),
                                new Vgtid.ShardGtid(TEST_KEYSPACE, TEST_SHARD2, "new_gtid2"))));
    }

    @Test
    public void shouldGetOrderedTransactionContext() {
        VitessConnectorConfig config = new VitessConnectorConfig(
                TestHelper.defaultConfig()
                        .with(VitessConnectorConfig.PROVIDE_ORDERED_TRANSACTION_METADATA, true)
                        .build());
        VitessOffsetContext.Loader loader = new VitessOffsetContext.Loader(config);
        Map offsets = Map.of(SourceInfo.VGTID_KEY, VGTID_JSON);
        VitessOffsetContext context = loader.load(offsets);
        TransactionContext transactionContext = context.getTransactionContext();
        assertThat(transactionContext).isInstanceOf(OrderedTransactionContext.class);
    }

    @Test
    public void shouldGetInitialOrderedTransactionContext() {
        VitessConnectorConfig config = new VitessConnectorConfig(
                TestHelper.defaultConfig()
                        .with(VitessConnectorConfig.PROVIDE_ORDERED_TRANSACTION_METADATA, true)
                        .build());
        VitessOffsetContext context = VitessOffsetContext.initialContext(config, Clock.system());
        TransactionContext transactionContext = context.getTransactionContext();
        assertThat(transactionContext).isInstanceOf(OrderedTransactionContext.class);
    }
}
