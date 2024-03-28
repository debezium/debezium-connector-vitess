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
import io.debezium.pipeline.txmetadata.OrderedTransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.spi.schema.DataCollectionId;

public class VitessOrderedTransactionStructMaker extends AbstractTransactionStructMaker implements TransactionStructMaker {

    @Override
    public Struct prepareTxStruct(OffsetContext offsetContext, DataCollectionId source) {
        Struct struct = super.prepareTxStruct(offsetContext, source);
        return addOrderMetadata(struct, offsetContext);
    }

    private Struct addOrderMetadata(Struct struct, OffsetContext offsetContext) {
        VitessTransactionOrderMetadata metadata = getVitessTransactionOrderMetadata(offsetContext);
        struct.put(VitessTransactionOrderMetadata.OFFSET_TRANSACTION_RANK, metadata.transactionRank.toString());
        struct.put(VitessTransactionOrderMetadata.OFFSET_TRANSACTION_EPOCH, metadata.transactionEpoch);
        return struct;
    }

    private VitessTransactionOrderMetadata getVitessTransactionOrderMetadata(OffsetContext offsetContext) {
        OrderedTransactionContext context = (OrderedTransactionContext) offsetContext.getTransactionContext();
        return (VitessTransactionOrderMetadata) context.getTransactionOrderMetadata();
    }

    @Override
    public Schema getTransactionBlockSchema() {
        return VitessSchemaFactory.get().getOrderedTransactionBlockSchema();
    }
}
