/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/** Create the source struct in the SourceRecord */
public class VitessSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public VitessSourceInfoStructMaker(
                                       String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        this.schema = commonSchemaBuilder()
                .name("io.debezium.connector.vitess.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.VGTID_KEYSPACE, Schema.STRING_SCHEMA)
                .field(SourceInfo.VGTID_SHARD, Schema.STRING_SCHEMA)
                .field(SourceInfo.VGTID_GTID, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct res = super.commonStruct(sourceInfo)
                .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table())
                .put(SourceInfo.VGTID_KEYSPACE, sourceInfo.getCurrentVgtid().getKeyspace())
                .put(SourceInfo.VGTID_SHARD, sourceInfo.getCurrentVgtid().getShard())
                .put(SourceInfo.VGTID_GTID, sourceInfo.getCurrentVgtid().getGtid());

        return res;
    }
}
