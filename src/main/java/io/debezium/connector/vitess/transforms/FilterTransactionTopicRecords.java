/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.transforms;

import static io.debezium.config.CommonConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import io.debezium.Module;
import io.debezium.config.Configuration;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;

public class FilterTransactionTopicRecords<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private SchemaNameAdjuster schemaNameAdjuster;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public R apply(R record) {
        if (isTransactionTopicMessage(record)) {
            return null;
        }
        return record;
    }

    private boolean isTransactionTopicMessage(R record) {
        if (record.keySchema().equals(SchemaFactory.get().transactionKeySchema(schemaNameAdjuster)) &&
                record.valueSchema().equals(SchemaFactory.get().transactionValueSchema(schemaNameAdjuster))) {
            return true;
        }
        return false;
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
        schemaNameAdjuster = VitessConnectorConfig.SchemaNameAdjustmentMode.parse(config.getString(SCHEMA_NAME_ADJUSTMENT_MODE))
                .createAdjuster();
    }
}
