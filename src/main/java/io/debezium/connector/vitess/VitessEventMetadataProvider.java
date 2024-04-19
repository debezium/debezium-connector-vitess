/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.vitess.pipeline.txmetadata.VitessTransactionInfo;
import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionInfo;
import io.debezium.spi.schema.DataCollectionId;
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
                SourceInfo.VGTID_KEY,
                sourceInfo.getString(SourceInfo.VGTID_KEY));
    }

    @Override
    public String getTransactionId(
                                   DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        // no transaction id if not a row-change event
        if (value == null || source == null) {
            return null;
        }

        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        // Use the entire VGTID as transaction id
        return sourceInfo.getString(SourceInfo.VGTID_KEY);
    }

    @Override
    public TransactionInfo getTransactionInfo(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null || source == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        String vgtid = sourceInfo.getString(SourceInfo.VGTID_KEY);
        String shard = sourceInfo.getString(SourceInfo.SHARD_KEY);
        return new VitessTransactionInfo(vgtid, shard);
    }

}
