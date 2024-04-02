/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;

public class VitessSchemaFactory extends SchemaFactory {

    public VitessSchemaFactory() {
        super();
    }

    private static final VitessSchemaFactory vitessSchemaFactoryObject = new VitessSchemaFactory();

    public static VitessSchemaFactory get() {
        return vitessSchemaFactoryObject;
    }

    public Schema getOrderedTransactionBlockSchema() {
        return SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, Schema.INT64_SCHEMA)
                .field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK, Schema.STRING_SCHEMA)
                .build();
    }
}
