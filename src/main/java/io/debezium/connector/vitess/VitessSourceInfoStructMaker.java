/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.AbstractSourceInfoStructMaker;

/** Create the source struct in the SourceRecord */
public class VitessSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public VitessSourceInfoStructMaker(
                                       String connector, String version, VitessConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        this.schema = commonSchemaBuilder()
                .name("io.debezium.connector.vitess.Source")
                .field(SourceInfo.KEYSPACE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.VGTID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct res = super.commonStruct(sourceInfo)
                .put(SourceInfo.KEYSPACE_NAME_KEY, sourceInfo.database())
                // Override DATABASE_NAME_KEY to empty string and in favor of KEYSPACE_NAME_KEY
                .put(SourceInfo.DATABASE_NAME_KEY, "")
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table())
                .put(SourceInfo.VGTID_KEY, sourceInfo.getCurrentVgtid().toString());
        return res;
    }
}
