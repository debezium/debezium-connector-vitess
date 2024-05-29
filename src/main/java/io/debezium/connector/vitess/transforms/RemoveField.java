/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.transforms.SmtManager;

public class RemoveField<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final String FIELD_DELIMITER = ",";

    public static final String FIELD_NAMES_CONF = "field_names";

    public static final Field FIELD_NAMES_FIELD = Field.create(FIELD_NAMES_CONF)
            .withDisplayName("List of field names to remove, full path eg source.database or transaction.id")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(RemoveField::validateRemoveFieldNames)
            .withDescription(
                    "The comma-separated list of fields to remove, e.g., 'source.id', 'transaction.data_collection_order'");

    protected List<String> fieldNames;

    private static int validateRemoveFieldNames(Configuration configuration, Field field, Field.ValidationOutput problems) {
        // ensure not empty and doesn't start with periods and doesn't end with periods
        String fieldNames = configuration.getString(field);
        if (fieldNames == null || fieldNames.isEmpty()) {
            problems.accept(field, fieldNames, "Field names cannot be empty or null, must specify field names to drop");
            return 1;
        }
        for (String fieldName : fieldNames.split(FIELD_DELIMITER)) {
            if (fieldName.startsWith(".") || fieldName.endsWith(".")) {
                problems.accept(field, fieldNames, "Field names cannot start or end with '.', must specify correct field name");
                return 1;
            }
        }
        return 0;
    }

    @Override
    public R apply(R record) {
        Struct value = (Struct) record.value();
        Schema schema = record.valueSchema();
        Schema updatedSchema = updateSchema("", schema);
        Struct newValue = updateStruct("", updatedSchema, value);
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, newValue, record.timestamp());
    }

    private Schema updateSchema(String fullName, Schema schema) {
        // Create a schema builder
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().version(schema.version()).name(schema.name());
        if (schema.isOptional()) {
            schemaBuilder = schemaBuilder.optional();
        }

        // Iterate over fields in the original schema and add to the schema builder dynamically
        for (org.apache.kafka.connect.data.Field field : schema.fields()) {
            String currentFullName = !fullName.isEmpty() ? fullName + "." + field.name() : field.name();

            if (field.schema().type() == Schema.Type.STRUCT) {
                // If the field is a nested struct, recursively modify it and add its schema
                Schema updatedNestedSchema = updateSchema(currentFullName, field.schema());
                schemaBuilder.field(field.name(), updatedNestedSchema);
            }
            else {
                if (!shouldExcludeField(currentFullName)) {
                    schemaBuilder.field(field.name(), field.schema());
                }
            }
        }
        return schemaBuilder.build();
    }

    private boolean shouldExcludeField(String fullFieldName) {
        for (String fieldName : fieldNames) {
            if (fullFieldName.equals(fieldName)) {
                return true;
            }
        }
        return false;
    }

    private Struct updateStruct(String fullName, Schema updatedSchema, Struct struct) {
        // Create an updated struct
        Struct updatedStruct = new Struct(updatedSchema);
        for (org.apache.kafka.connect.data.Field field : updatedSchema.fields()) {
            String currentFullName = fullName != "" ? fullName + "." + field.name() : field.name();
            Object fieldValue = struct.get(field.name());
            if (fieldValue instanceof Struct) {
                // If a field is a struct recursively create its nested structs
                Struct nestedStruct = (Struct) fieldValue;
                Struct updatedNestedStruct = updateStruct(currentFullName, field.schema(), nestedStruct);
                updatedStruct.put(field, updatedNestedStruct);
            }
            else {
                if (!shouldExcludeField(currentFullName)) {
                    updatedStruct.put(field, fieldValue);
                }
            }
        }
        return updatedStruct;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        Field.group(config, null, FIELD_NAMES_FIELD);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> props) {
        final Configuration config = Configuration.from(props);
        SmtManager<R> smtManager = new SmtManager<>(config);
        smtManager.validate(config, Field.setOf(FIELD_NAMES_FIELD));
        fieldNames = determineRemoveFields(config);
    }

    private static List<String> determineRemoveFields(Configuration config) {
        String fieldNamesString = config.getString(FIELD_NAMES_FIELD);
        List<String> fieldNames = new ArrayList<>();
        for (String fieldName : fieldNamesString.split(FIELD_DELIMITER)) {
            fieldNames.add(fieldName);
        }
        return fieldNames;
    }
}
