/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.util.VersionParser;

public class Module {

    public static String version() {
        return VersionParser.getVersion();
    }

    /** @return name of the connector plugin */
    public static String name() {
        return "vitess";
    }

    /** @return context name used in log MDC and JMX metrics */
    public static String contextName() {
        return "Vitess";
    }
}
