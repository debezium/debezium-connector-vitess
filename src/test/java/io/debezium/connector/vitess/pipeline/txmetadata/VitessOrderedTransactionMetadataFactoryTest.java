/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class VitessOrderedTransactionMetadataFactoryTest {

    @Test
    public void shouldGetDefaultTransactionContext() {
        Configuration config = TestHelper.defaultConfig().build();
        TransactionContext context = new VitessOrderedTransactionMetadataFactory(config).getTransactionContext();
        assertThat(context).isInstanceOf(VitessOrderedTransactionContext.class);
    }

    @Test
    public void shouldGetTransactionContextWithShardEpochMapFromConfig() {
        Configuration config = Configuration.create()
                .with(VitessConnectorConfig.VITESS_TASK_SHARD_EPOCH_MAP_CONFIG, TestHelper.TEST_SHARD_TO_EPOCH)
                .with(VitessConnectorConfig.OFFSET_STORAGE_PER_TASK, "true")
                .with(VitessConnectorConfig.VITESS_TASK_VGTID_CONFIG, VgtidTest.VGTID_JSON)
                .build();
        TransactionContext context = new VitessOrderedTransactionMetadataFactory(config).getTransactionContext();
        VitessOrderedTransactionContext orderedTransactionContext = (VitessOrderedTransactionContext) context;
        orderedTransactionContext.beginTransaction(new VitessTransactionInfo(VgtidTest.VGTID_JSON, TestHelper.TEST_SHARD1));
        assertThat(orderedTransactionContext.getTransactionEpoch()).isEqualTo(TestHelper.TEST_SHARD1_EPOCH);
    }

    @Test
    public void getTransactionStructMaker() {
    }
}
