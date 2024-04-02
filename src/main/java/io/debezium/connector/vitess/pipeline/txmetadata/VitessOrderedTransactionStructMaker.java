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

    /**
     * Adds the transaction block to a change log message. Transaction block example:
     * "transaction": {
     *     "id": "[{\"keyspace\":\ks1\",\"shard\":\"-80\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]},
     *             {\"keyspace\":\ks1\",\"shard\":\"80-\",\"gtid\":\"MySQL56/host1:123,host2:234\",\"table_p_ks\":[]}",
     *     "total_order": 1,
     *     "data_collection_order": 1,
     *     "transaction_epoch": 0,
     *     "transaction_rank": 853
     * }
     * @param offsetContext
     * @param dataCollectionEventOrder
     * @param value
     * @return Struct with ordered transaction metadata
     */
    @Override
    public Struct prepareTxStruct(OffsetContext offsetContext, long dataCollectionEventOrder, Struct value) {
        Struct struct = super.prepareTxStruct(offsetContext, dataCollectionEventOrder, value);
        return addOrderMetadata(struct, offsetContext);
    }

    private Struct addOrderMetadata(Struct struct, OffsetContext offsetContext) {
        VitessOrderedTransactionContext context = getVitessTransactionOrderMetadata(offsetContext);
        struct.put(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK, context.transactionRank);
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
