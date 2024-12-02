/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;

/**
 * @author Thomas Thornton
 */
public class DdlMetadataExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DdlMetadataExtractor.class);

    // VStream DDL statements do not contain any database/keyspace, only contains the table name
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
            "(?i)(CREATE|ALTER|TRUNCATE|DROP|RENAME)\\s+TABLE\\s+['\\\"`]?([\\w]+)['\\\"`]?",
            Pattern.CASE_INSENSITIVE);

    // Regex to match in-line or multi-line comments (e.g., /* comment */)
    private static final Pattern COMMENT_PATTERN = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);

    private static final String UNKNOWN_TABLE_NAME = "<UNKNOWN>";

    private final DdlMessage ddlMessage;
    private String operation;
    private String table;

    public DdlMetadataExtractor(ReplicationMessage ddlMessage) {
        this.ddlMessage = (DdlMessage) ddlMessage;
        extractMetadata();
    }

    public void extractMetadata() {
        String cleanedStatement = removeComments(this.ddlMessage.getStatement());
        Matcher matcher = TABLE_NAME_PATTERN.matcher(cleanedStatement);
        if (matcher.find()) {
            operation = matcher.group(1).split("\s+")[0].toUpperCase();
            if (operation.equals("RENAME")) {
                operation = "ALTER";
            }
            table = matcher.group(2);
        }
    }

    private String removeComments(String statement) {
        return COMMENT_PATTERN.matcher(statement).replaceAll("");
    }

    public SchemaChangeEvent.SchemaChangeEventType getSchemaChangeEventType() {
        if (operation == null) {
            logUnknownMessage("schema change event type");
            // An event type is required to build a schema change event, so if we got an empty event type, default to ALTER
            return SchemaChangeEvent.SchemaChangeEventType.ALTER;
        }
        return SchemaChangeEvent.SchemaChangeEventType.valueOf(operation);
    }

    public String getTable() {
        if (table == null) {
            logUnknownMessage("table");
            table = UNKNOWN_TABLE_NAME;
        }
        return VitessDatabaseSchema.buildTableId(ddlMessage.getShard(), ddlMessage.getKeyspace(), table).toDoubleQuotedString();
    }

    private void logUnknownMessage(String message) {
        LOGGER.warn("Unknown {}, keyspace: {}, shard: {}, commit time {}, transaction ID: {}",
                message,
                ddlMessage.getKeyspace(),
                ddlMessage.getShard(),
                ddlMessage.getCommitTime(),
                ddlMessage.getTransactionId());
    }
}
