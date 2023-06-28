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

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        this.schema = commonSchemaBuilder()
                .name("io.debezium.connector.vitess.Source")
                .field(SourceInfo.KEYSPACE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SHARD_KEY, Schema.STRING_SCHEMA)
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
                .put(SourceInfo.KEYSPACE_NAME_KEY, sourceInfo.keyspace())
                .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table())
                .put(SourceInfo.SHARD_KEY, sourceInfo.shard())
                .put(SourceInfo.VGTID_KEY, sourceInfo.getCurrentVgtid().toString());
        return res;
    }
}
