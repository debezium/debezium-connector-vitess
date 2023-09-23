/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess.converters;

import java.util.Set;

import org.apache.kafka.connect.errors.DataException;

import io.debezium.connector.vitess.SourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.RecordParser;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * Parser for records produced by the Vitess connector.
 *
 * @author Chris Cranford
 */
public class VitessRecordParser extends RecordParser {

    private static final Set<String> VITESS_SOURCE_FIELD = Collect.unmodifiableSet(SourceInfo.VGTID_KEY, SourceInfo.KEYSPACE_NAME_KEY);

    public VitessRecordParser(RecordAndMetadata recordAndMetadata) {
        super(recordAndMetadata, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public Object getMetadata(String name) {
        if (SOURCE_FIELDS.contains(name)) {
            return source().get(name);
        }
        if (VITESS_SOURCE_FIELD.contains(name)) {
            return source().get(name);
        }

        throw new DataException("No such field \"" + name + "\" in the \"source\" field of events from Vitess connector");
    }
}
