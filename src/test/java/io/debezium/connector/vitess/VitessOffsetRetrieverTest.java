/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;

public class VitessOffsetRetrieverTest {

    @Test
    public void isShardEpochMapEnabled() {
        Configuration configuration = Configuration.create()
                .with(VitessConnectorConfig.TRANSACTION_METADATA_FACTORY, VitessOrderedTransactionMetadataFactory.class).build();
        VitessConnectorConfig connectorConfig = new VitessConnectorConfig(configuration);
        assertThat(VitessOffsetRetriever.isShardEpochMapEnabled(connectorConfig)).isTrue();
    }
}
