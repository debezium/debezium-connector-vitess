/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessSchemaFactory extends SchemaFactory {

    private static final String VITESS_HEARTBEAT_VALUE_SCHEMA_NAME = "io.debezium.connector.vitess.Heartbeat";
    private static final int VITESS_HEARTBEAT_VALUE_SCHEMA_VERSION = 1;

    public VitessSchemaFactory() {
        super();
    }

    private static final VitessSchemaFactory vitessSchemaFactoryObject = new VitessSchemaFactory();

    public static VitessSchemaFactory get() {
        return vitessSchemaFactoryObject;
    }

    public Schema getOrderedTransactionBlockSchema() {
        Schema rankSchema = Decimal.schema(0).schema();
        return SchemaBuilder.struct().optional()
                .name(TRANSACTION_BLOCK_SCHEMA_NAME)
                .version(TRANSACTION_BLOCK_SCHEMA_VERSION)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
                .field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, Schema.INT64_SCHEMA)
                .field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK, rankSchema)
                .build();
    }

    @Override
    public Schema heartbeatValueSchema(SchemaNameAdjuster adjuster) {
        return SchemaBuilder.struct()
                .name(adjuster.adjust(VITESS_HEARTBEAT_VALUE_SCHEMA_NAME))
                .version(VITESS_HEARTBEAT_VALUE_SCHEMA_VERSION)
                .field(SourceInfo.VGTID_KEY, Schema.STRING_SCHEMA)
                .field(AbstractSourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .build();

    }
}
