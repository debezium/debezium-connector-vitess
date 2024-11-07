/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.connection;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.debezium.connector.vitess.VitessDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;

/**
 * @author Thomas Thornton
 */
public class DdlMetadataExtractor {

    // VStream DDL statements do not contain any database/keyspace, only contains the table name
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile(
            "(?i)(CREATE|ALTER|TRUNCATE|DROP|RENAME)\\s+TABLE\\s+['\\\"`]?([\\w]+)['\\\"`]?",
            Pattern.CASE_INSENSITIVE);

    private final DdlMessage ddlMessage;
    private String operation;
    private String table;

    public DdlMetadataExtractor(ReplicationMessage ddlMessage) {
        this.ddlMessage = (DdlMessage) ddlMessage;
        extractMetadata();
    }

    public void extractMetadata() {
        Matcher matcher = TABLE_NAME_PATTERN.matcher(this.ddlMessage.getStatement());
        if (matcher.find()) {
            operation = matcher.group(1).split("\s+")[0].toUpperCase();
            if (operation.equals("RENAME")) {
                operation = "ALTER";
            }
            table = matcher.group(2);
        }
    }

    public SchemaChangeEvent.SchemaChangeEventType getSchemaChangeEventType() {
        return SchemaChangeEvent.SchemaChangeEventType.valueOf(operation);
    }

    public String getTable() {
        return VitessDatabaseSchema.buildTableId(ddlMessage.getShard(), ddlMessage.getKeyspace(), table).toDoubleQuotedString();
    }
}
