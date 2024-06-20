/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessSchemaFactoryTest {

    @Test
    public void get() {
        assertThat(VitessSchemaFactory.get()).isNotNull();
    }

    @Test
    public void getOrderedTransactionBlockSchema() {
        Schema orderedTransactionBlockSchema = VitessSchemaFactory.get().getOrderedTransactionBlockSchema();
        List<Field> fields = orderedTransactionBlockSchema.fields();
        assertThat(fields).contains(new Field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, 0, Schema.STRING_SCHEMA));
        assertThat(fields).contains(new Field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, 1, Schema.INT64_SCHEMA));
        assertThat(fields).contains(new Field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, 2, Schema.INT64_SCHEMA));
        assertThat(fields).contains(new Field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_EPOCH, 3, Schema.INT64_SCHEMA));
        assertThat(fields).contains(new Field(VitessOrderedTransactionContext.OFFSET_TRANSACTION_RANK, 4, Decimal.schema(0)));
    }

    @Test
    public void getHeartbeatValueSchema() {
        Schema heartbeatValueSchema = VitessSchemaFactory.get().heartbeatValueSchema(SchemaNameAdjuster.NO_OP);
        List<Field> fields = heartbeatValueSchema.fields();
        assertThat(fields).contains(new Field(SourceInfo.VGTID_KEY, 0, Schema.STRING_SCHEMA));
        assertThat(fields).contains(new Field(AbstractSourceInfo.TIMESTAMP_KEY, 1, Schema.INT64_SCHEMA));
    }

    @Test
    public void datatypeEnvelopeSchema() {
    }
}
