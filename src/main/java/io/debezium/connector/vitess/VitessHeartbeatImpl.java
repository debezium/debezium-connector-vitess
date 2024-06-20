/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.function.BlockingConsumer;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.schema.SchemaNameAdjuster;

public class VitessHeartbeatImpl extends HeartbeatImpl implements Heartbeat {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessHeartbeatImpl.class);

    private final String topicName;
    private final String key;

    private final Schema keySchema;
    private final Schema valueSchema;

    public VitessHeartbeatImpl(Duration heartbeatInterval, String topicName, String key, SchemaNameAdjuster schemaNameAdjuster) {
        super(heartbeatInterval, topicName, key, schemaNameAdjuster);
        this.topicName = topicName;
        this.key = key;
        keySchema = VitessSchemaFactory.get().heartbeatKeySchema(schemaNameAdjuster);
        valueSchema = VitessSchemaFactory.get().heartbeatValueSchema(schemaNameAdjuster);
    }

    @Override
    public void forcedBeat(Map<String, ?> partition, Map<String, ?> offset, BlockingConsumer<SourceRecord> consumer)
            throws InterruptedException {
        LOGGER.debug("Generating heartbeat event");
        if (offset == null || offset.isEmpty()) {
            // Do not send heartbeat message if no offset is available yet
            return;
        }
        consumer.accept(heartbeatRecord(partition, offset));
    }

    private SourceRecord heartbeatRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset) {
        final Integer partition = 0;

        return new SourceRecord(sourcePartition, sourceOffset,
                topicName, partition, keySchema, serverNameKey(key), valueSchema, messageValue(sourceOffset));
    }

    private Struct serverNameKey(String serverName) {
        Struct result = new Struct(keySchema);
        result.put(SERVER_NAME_KEY, serverName);
        return result;
    }

    private Struct messageValue(Map<String, ?> sourceOffset) {
        String vgtid = (String) sourceOffset.get(SourceInfo.VGTID_KEY);
        if (vgtid == null) {
            vgtid = "";
        }
        Struct result = new Struct(valueSchema);
        result.put(SourceInfo.VGTID_KEY, vgtid);
        result.put(AbstractSourceInfo.TIMESTAMP_KEY, Instant.now().toEpochMilli());
        return result;
    }
}
