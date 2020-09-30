/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Collect;

/** Used for metrics collection. */
public class VitessEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(
                                     DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null || source == null) {
            return null;
        }

        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);

        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Instant.ofEpochMilli(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(
                                                      DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }

        return Collect.hashMapOf(
                SourceInfo.VGTID_KEYSPACE,
                sourceInfo.getString(SourceInfo.VGTID_KEYSPACE),
                SourceInfo.VGTID_SHARD,
                sourceInfo.getString(SourceInfo.VGTID_SHARD),
                SourceInfo.VGTID_GTID,
                sourceInfo.getString(SourceInfo.VGTID_GTID));
    }

    @Override
    public String getTransactionId(
                                   DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // no transaction id if not a row-change event
        if (value == null || source == null) {
            return null;
        }

        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        // Use GTID as transaction id, if further granularity is needed, add keyspace + shard
        return sourceInfo.getString(SourceInfo.VGTID_GTID);
    }

}
