/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.transforms.SmtManager;

/**
 * A transform for replacing the value of a field with a constant value. Reads from configs the list
 * of field names and the constant value to use to overwrite the original value.
 *
 * A possible use case is clearing a field that is very large and thus degrades throughput, e.g., vgtid.
 *
 * @author Thomas Thornton
 */
public class ReplaceFieldValue<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final String FIELD_DELIMITER = ",";

    public static final String FIELD_NAMES_CONF = "field_names";
    public static final String FIELD_VALUE_CONF = "field_value";

    public static final Field FIELD_NAMES_FIELD = Field.create(FIELD_NAMES_CONF)
            .withDisplayName("List of field names to replace values, full path eg source.database or transaction.id")
            .withType(ConfigDef.Type.LIST)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(ReplaceFieldValue::validateReplaceFieldValueNames)
            .withDescription(
                    "The comma-separated list of fields to replace, e.g., 'source.id', 'transaction.data_collection_order'");

    public static final Field FIELD_VALUE_FIELD = Field.create(FIELD_VALUE_CONF)
            .withDisplayName("The static value that will be used to set the replaced field")
            .withType(ConfigDef.Type.LIST)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription(
                    "The value that is used to overwrite the field, defaults to empty string");

    protected Set<String> fieldNames;
    private String fieldValue;

    private static int validateReplaceFieldValueNames(Configuration configuration, Field field, Field.ValidationOutput problems) {
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
        for (String field : fieldNames) {
            updateStruct(field, fieldValue, value);
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                schema, value, record.timestamp());
    }

    private Struct updateStruct(String pathToReplace, String previousPath, Struct struct) {
        if (pathToReplace.isEmpty()) {
            throw new RuntimeException("Cannot replace empty field name");
        }
        int dotIndex = pathToReplace.indexOf('.');
        if (dotIndex != -1) {
            // A nested field
            String currentField = pathToReplace.substring(0, dotIndex);
            String remainingPath = pathToReplace.substring(dotIndex + 1);
            String currentPath = !previousPath.isEmpty() ? String.join(".", previousPath, currentField) : currentField;
            if (struct.schema().field(currentField) != null) {
                Object fieldValue = struct.get(currentField);
                if (fieldValue instanceof Struct) {
                    // If a field is a struct it will be modified during the recursive calls
                    Struct nestedStruct = (Struct) fieldValue;
                    return updateStruct(remainingPath, currentPath, nestedStruct);
                }
            }
            // The path is not present so do not modify the struct
            return struct;
        }
        else {
            struct.put(pathToReplace, fieldValue);
            return struct;
        }
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
        fieldNames = determineReplaceFieldValues(config);
        fieldValue = config.getString(FIELD_VALUE_FIELD);
    }

    private static Set<String> determineReplaceFieldValues(Configuration config) {
        String fieldNamesString = config.getString(FIELD_NAMES_FIELD);
        Set<String> fieldNames = new HashSet<>();
        for (String fieldName : fieldNamesString.split(FIELD_DELIMITER)) {
            fieldNames.add(fieldName);
        }
        return fieldNames;
    }
}
