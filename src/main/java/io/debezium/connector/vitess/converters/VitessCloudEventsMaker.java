/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.vitess.SourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records produced by the Vitess connector.
 *
 * @author Chris Cranford
 */
public class VitessCloudEventsMaker extends CloudEventsMaker {

    private static final Set<String> VITESS_SOURCE_FIELDS = Collect.unmodifiableSet(SourceInfo.VGTID_KEY, SourceInfo.KEYSPACE_NAME_KEY);

    public VitessCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType contentType, String dataSchemaUriBase, String cloudEventsSchemaName) {
        super(recordAndMetadata, contentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";vgtid:" + sourceField(SourceInfo.VGTID_KEY);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return VITESS_SOURCE_FIELDS;
    }
}
