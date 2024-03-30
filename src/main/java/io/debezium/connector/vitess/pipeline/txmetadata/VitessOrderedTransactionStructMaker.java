/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.pipeline.txmetadata;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.vitess.VitessSchemaFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.AbstractTransactionStructMaker;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;

public class VitessOrderedTransactionStructMaker extends AbstractTransactionStructMaker implements TransactionStructMaker {

    @Override
    public Struct prepareTxStruct(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value) {
        Struct struct = super.prepareTxStruct(offsetContext, dataCollectionEventOrder, value);
        return addOrderMetadata(struct, offsetContext);
    }

    private Struct addOrderMetadata(Struct struct, OffsetContext offsetContext) {
        VitessOrderedTransactionContext context = getVitessTransactionOrderMetadata(offsetContext);
        struct.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK, context.transactionRank.toString());
        struct.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, context.transactionEpoch);
        return struct;
    }

    private VitessOrderedTransactionContext getVitessTransactionOrderMetadata(OffsetContext offsetContext) {
        return (VitessOrderedTransactionContext) offsetContext.getTransactionContext();
    }

    @Override
    public Schema getTransactionBlockSchema() {
        return VitessSchemaFactory.get().getOrderedTransactionBlockSchema();
    }
}
