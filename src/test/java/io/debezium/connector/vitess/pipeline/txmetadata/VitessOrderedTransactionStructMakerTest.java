/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;

import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import io.debezium.connector.vitess.TestHelper;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.connector.vitess.VgtidTest;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.connector.vitess.VitessOffsetContext;
import io.debezium.connector.vitess.VitessSchemaFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;

public class VitessOrderedTransactionStructMakerTest {

    @Test
    public void prepareTxStruct() {
        VitessConnectorConfig config = new VitessConnectorConfig(TestHelper.defaultConfig().build());
        VitessOrderedTransactionStructMaker maker = new VitessOrderedTransactionStructMaker();
        TransactionContext transactionContext = new VitessOrderedTransactionContext();
        transactionContext.beginTransaction(new VitessTransactionInfo(VgtidTest.VGTID_JSON, VgtidTest.TEST_SHARD));
        OffsetContext context = new VitessOffsetContext(config, Vgtid.of(VgtidTest.VGTID_JSON), Instant.now(), transactionContext);
        Struct struct = maker.prepareTxStruct(context, 0, null);
        assertThat(struct.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH)).isEqualTo(0L);
        assertThat(struct.get(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK)).isEqualTo(new BigDecimal(1513));
    }

    @Test
    public void getTransactionBlockSchema() {
        VitessOrderedTransactionStructMaker maker = new VitessOrderedTransactionStructMaker();
        assertThat(maker.getTransactionBlockSchema()).isEqualTo(VitessSchemaFactory.get().getOrderedTransactionBlockSchema());
    }
}
