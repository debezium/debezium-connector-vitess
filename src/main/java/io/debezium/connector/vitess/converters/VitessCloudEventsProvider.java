/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.converters;

import io.debezium.connector.vitess.transforms.VitessAbstractRecordParserProvider;

import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.CloudEventsProvider;
import io.debezium.converters.spi.RecordParser;
import io.debezium.converters.spi.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Vitess.
 *
 * @author Chris Cranford
 */
public class VitessCloudEventsProvider extends VitessAbstractRecordParserProvider implements CloudEventsProvider {
    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new VitessCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}
