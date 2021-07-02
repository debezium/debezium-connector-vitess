/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.converters;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.vitess.Module;
import io.debezium.converters.CloudEventsMaker;
import io.debezium.converters.CloudEventsProvider;
import io.debezium.converters.RecordParser;
import io.debezium.converters.SerializerType;

/**
 * An implementation of {@link CloudEventsProvider} for Vitess.
 *
 * @author Chris Cranford
 */
public class VitessCloudEventsProvider implements CloudEventsProvider {
    @Override
    public String getName() {
        return Module.name();
    }

    @Override
    public RecordParser createParser(Schema schema, Struct record) {
        return new VitessRecordParser(schema, record);
    }

    @Override
    public CloudEventsMaker createMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        return new VitessCloudEventsMaker(parser, contentType, dataSchemaUriBase);
    }
}
