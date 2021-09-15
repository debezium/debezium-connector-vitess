/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.config.ConfigDefinitionMetadataTest;

public class VitessConnectorConfigDefTest extends ConfigDefinitionMetadataTest {

    public VitessConnectorConfigDefTest() {
        super(new VitessConnector());
    }
}
