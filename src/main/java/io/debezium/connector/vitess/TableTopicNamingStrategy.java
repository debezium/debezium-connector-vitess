/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.Properties;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.relational.Selectors;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Collect;
import io.debezium.util.Strings;

/**
 * <p>
 * Topic naming strategy where only the table name is added. This is used to avoid including
 * the shard which is now part of the catalog of the table ID and would be included if
 * the DefaultTopicNamingStrategy is being used.
 *</p>
 * Additionally, supports some Vitess-specific configs:
 * <ul>
 *     <li>
 *        overrideDataChangeTopicPrefix: in the case of mulitple connectors for the same keyspace,
 *        a unique `topic.prefix` is required for proper metric reporting, so in order for a consistent topic
 *        naming convention, the data change topic prefix can be set here (typically shared between connectors of the same
 *        keyspace
 *      </li>
 *      <li>
 *        overrideSchemaChangeTopic: in the case of multiple connectors for the same keyspace and `include.schema.changes` being enabled,
 *        this is used to prevent the ddl events from being written to the topic named `topic.prefix`, which is the default behavior.
 *        The reason why this is necessary is that the `topic.prefix` may include the table name for uniqueness, so it may actually be the name
 *        of a data change topic.
 *      </li>
 * </ul>
 */
public class TableTopicNamingStrategy extends AbstractTopicNamingStrategy<TableId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableTopicNamingStrategy.class);

    public static final Field OVERRIDE_DATA_CHANGE_TOPIC_PREFIX = Field.create("override.data.change.topic.prefix")
            .withDisplayName("Override Data Topic prefix")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(CommonConnectorConfig::validateTopicName)
            .withDescription("Overrides the topic.prefix used for the data change topic.");

    public static final Field OVERRIDE_DATA_CHANGE_TOPIC_PREFIX_EXCLUDE_LIST = Field.create("override.data.change.topic.prefix.exclude.list")
            .withDisplayName("Override data change topic prefix exclusion regex list")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(Field::isListOfRegex)
            .withDescription("Prevents overriding the topic.prefix used for the data change topic for tables matching the list of regular expressions. " +
                    "One use case is for not overriding the vitess heartbeat table.");

    public static final Field OVERRIDE_SCHEMA_CHANGE_TOPIC = Field.create("override.schema.change.topic")
            .withDisplayName("Override schema change topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(CommonConnectorConfig::validateTopicName)
            .withDescription("Overrides the name of the schema change topic (if not set uses topic.prefx).");

    private String overrideDataChangeTopicPrefix;
    private Tables.TableFilter overrideDataChangeTopicPrefixFilter;
    private String overrideSchemaChangeTopic;

    public TableTopicNamingStrategy(Properties props) {
        super(props);
    }

    @Override
    public void configure(Properties props) {
        super.configure(props);
        Configuration config = Configuration.from(props);
        final Field.Set configFields = Field.setOf(
                OVERRIDE_DATA_CHANGE_TOPIC_PREFIX,
                OVERRIDE_DATA_CHANGE_TOPIC_PREFIX_EXCLUDE_LIST,
                OVERRIDE_SCHEMA_CHANGE_TOPIC);
        if (!config.validateAndRecord(configFields, LOGGER::error)) {
            throw new ConnectException("Unable to validate config.");
        }

        overrideDataChangeTopicPrefix = config.getString(OVERRIDE_DATA_CHANGE_TOPIC_PREFIX);
        overrideDataChangeTopicPrefixFilter = Tables.TableFilter.fromPredicate(
                Selectors.tableSelector()
                        .excludeTables(
                                config.getString(OVERRIDE_DATA_CHANGE_TOPIC_PREFIX_EXCLUDE_LIST),
                                new VitessTableIdToStringMapper())
                        .build());
        overrideSchemaChangeTopic = config.getString(OVERRIDE_SCHEMA_CHANGE_TOPIC);
    }

    public static TableTopicNamingStrategy create(CommonConnectorConfig config) {
        return new TableTopicNamingStrategy(config.getConfig().asProperties());
    }

    @Override
    public String dataChangeTopic(TableId id) {
        String topicName;
        if (!Strings.isNullOrBlank(overrideDataChangeTopicPrefix) && overrideDataChangeTopicPrefixFilter.isIncluded(id)) {
            topicName = mkString(Collect.arrayListOf(overrideDataChangeTopicPrefix, id.table()), delimiter);
        }
        else {
            topicName = mkString(Collect.arrayListOf(prefix, id.table()), delimiter);
        }
        return topicNames.computeIfAbsent(id, t -> sanitizedTopicName(topicName));
    }

    /**
     * Return the schema change topic. There are two cases:
     * 1. If override schema change topic is specified - use this as the topic name
     * 2. If override schema change topic is not specified - call the super method to get the typical
     * schema change topic name.
     *
     * @return String representing the schema change topic name.
     */
    @Override
    public String schemaChangeTopic() {
        if (!Strings.isNullOrBlank(overrideSchemaChangeTopic)) {
            return overrideSchemaChangeTopic;
        }
        else {
            return super.schemaChangeTopic();
        }
    }
}
