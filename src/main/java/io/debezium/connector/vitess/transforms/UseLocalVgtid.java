/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static io.debezium.connector.vitess.SourceInfo.SHARD_KEY;
import static io.debezium.connector.vitess.SourceInfo.VGTID_KEY;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.config.Configuration;
import io.debezium.connector.vitess.Module;
import io.debezium.connector.vitess.Vgtid;
import io.debezium.transforms.SmtManager;

public class UseLocalVgtid<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private SmtManager<R> smtManager;

    @Override
    public R apply(R record) {
        if (record.value() instanceof Struct) {
            Struct value = (Struct) record.value();
            Schema schema = record.valueSchema();

            String localVgtid = getLocalVgtid(value);

            if (localVgtid != null) {
                Struct updatedValue = modifyStruct("", value, schema, localVgtid);
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                        schema, updatedValue, record.timestamp());
            }

        }
        return record;
    }

    private String getLocalVgtid(Struct value) {
        if (value.schema().field("source") != null) {
            Struct sourceStruct = (Struct) value.get("source");
            String shard = sourceStruct.getString(SHARD_KEY);
            String vgtid = sourceStruct.getString(VGTID_KEY);
            return Vgtid.of(vgtid).getLocalVgtid(shard).toString();
        }
        else {
            return null;
        }
    }

    private Struct modifyStruct(String previousPath, Struct struct, Schema schema, String localVgtid) {
        // change to work only with exact path match
        Struct updatedStruct = new Struct(schema);
        for (Field field : schema.fields()) {
            String fullName = previousPath.isEmpty() ? field.name() : previousPath + "." + field.name();
            Object fieldValue = struct.get(field);
            if (fieldValue instanceof Struct) {
                Struct nestedStruct = (Struct) fieldValue;
                Struct updatedNestedStruct = modifyStruct(fullName, nestedStruct, field.schema(), localVgtid);
                updatedStruct.put(field, updatedNestedStruct);
            }
            else {
                if (fullName.equals("source." + VGTID_KEY)) {
                    updatedStruct.put(field, localVgtid);
                }
                else {
                    updatedStruct.put(field, fieldValue);
                }
            }
        }
        return updatedStruct;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final Configuration config = Configuration.from(props);
        smtManager = new SmtManager<>(config);
    }

    @Override
    public String version() {
        return Module.version();
    }
}
