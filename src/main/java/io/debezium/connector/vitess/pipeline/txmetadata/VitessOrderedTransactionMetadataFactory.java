/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.pipeline.txmetadata;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.pipeline.txmetadata.spi.TransactionMetadataFactory;

public class VitessOrderedTransactionMetadataFactory implements TransactionMetadataFactory {

    private final Configuration configuration;

    public VitessOrderedTransactionMetadataFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public TransactionContext getTransactionContext() {
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        VitessOrderedTransactionContext context = VitessOrderedTransactionContext.initialize(connectorConfig);
        return context;
    }

    @Override
    public TransactionStructMaker getTransactionStructMaker() {
        return new VitessOrderedTransactionStructMaker(configuration);
    }
}
