/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.connector.vitess.pipeline.txmetadata.VitessTransactionOrderMetadata;
import io.debezium.data.Envelope;
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
                .field(VitessTransactionOrderMetadata.OFFSET_TRANSACTION_EPOCH, Schema.INT64_SCHEMA)
                .field(VitessTransactionOrderMetadata.OFFSET_TRANSACTION_RANK, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Envelope.Builder datatypeEnvelopeSchema() {
        return new Envelope.Builder() {
            private final SchemaBuilder builder = SchemaBuilder.struct()
                    .version(Envelope.SCHEMA_VERSION);

            private final Set<String> missingFields = new HashSet<>();

            @Override
            public Envelope.Builder withSchema(Schema fieldSchema, String... fieldNames) {
                for (String fieldName : fieldNames) {
                    builder.field(fieldName, fieldSchema);
                }
                return this;
            }

            @Override
            public Envelope.Builder withName(String name) {
                builder.name(name);
                return this;
            }

            @Override
            public Envelope.Builder withDoc(String doc) {
                builder.doc(doc);
                return this;
            }

            @Override
            public Envelope build() {
                builder.field(Envelope.FieldName.OPERATION, Envelope.OPERATION_REQUIRED ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP_US, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(Envelope.FieldName.TIMESTAMP_NS, Schema.OPTIONAL_INT64_SCHEMA);
                builder.field(Envelope.FieldName.TRANSACTION, getOrderedTransactionBlockSchema());
                checkFieldIsDefined(Envelope.FieldName.OPERATION);
                checkFieldIsDefined(Envelope.FieldName.BEFORE);
                checkFieldIsDefined(Envelope.FieldName.AFTER);
                checkFieldIsDefined(Envelope.FieldName.SOURCE);
                checkFieldIsDefined(Envelope.FieldName.TRANSACTION);
                if (!missingFields.isEmpty()) {
                    throw new IllegalStateException("The envelope schema is missing field(s) " + String.join(", ", missingFields));
                }
                return new Envelope(builder.build());
            }

            private void checkFieldIsDefined(String fieldName) {
                if (builder.field(fieldName) == null) {
                    missingFields.add(fieldName);
                }
            }
        };
    }
}
