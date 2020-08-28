/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Instant;

/** The return value does not matter */
public class AnonymousValue {

    public static String getString() {
        return "foo";
    }

    public static int getInt() {
        return 80;
    }

    public static long getLong() {
        return 0L;
    }

    public static Instant getInstant() {
        return Instant.now();
    };
}
