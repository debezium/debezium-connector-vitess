/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.converters;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.CloudEventsMaker;
import io.debezium.converters.RecordParser;
import io.debezium.converters.SerializerType;

/**
 * CloudEvents maker for records produced by the Vitess connector.
 *
 * @author Chris Cranford
 */
public class VitessCloudEventsMaker extends CloudEventsMaker {

    public VitessCloudEventsMaker(RecordParser parser, SerializerType contentType, String dataSchemaUriBase) {
        super(parser, contentType, dataSchemaUriBase);
    }

    @Override
    public String ceId() {
        return "name:" + recordParser.getMetadata(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";vgtid:" + recordParser.getMetadata(VitessRecordParser.VGTID_KEY);
    }
}
