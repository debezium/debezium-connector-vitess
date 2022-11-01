/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.data.VerifyRecord;

/**
 * An extension of {@link VerifyRecord} that verifies Vitess generated record
 */
public class VitessVerifyRecord extends VerifyRecord {

    /**
     * Verify that the given {@link SourceRecord} is an INSERT record, and that the key exists.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     */
    public static void isValidInsert(SourceRecord record, String pkField) {
        hasValidKey(record, pkField);
        isValidInsert(record, true);
    }

    /**
     * Verify that the given {@link SourceRecord} has a valid non-null integer key.
     *
     * @param record the source record; may not be null
     * @param pkField the single field defining the primary key of the struct; may not be null
     */
    public static void hasValidKey(SourceRecord record, String pkField) {
        Struct key = (Struct) record.key();
        assertThat(key.get(pkField)).isNotNull();
    }
}
