/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.vitess.jdbc;

import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjuster;
import java.util.List;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.binlog.jdbc.BinlogValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.vitess.VitessConnectorConfig;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.service.spi.ServiceRegistry;

/**
 * @author Thomas Thornton
 */
public class VitessBinlogValueConverter extends BinlogValueConverters {
    /**
     * Create a new instance of the value converters that always uses UTC for the default time zone when
     * converting values without timezone information to values that require timezones.
     *
     * @param decimalMode                        how {@code DECIMAL} and {@code NUMERIC} values are treated; can be null if {@link DecimalMode#PRECISE} is used
     * @param temporalPrecisionMode              temporal precision mode
     * @param bigIntUnsignedMode                 how {@code BIGINT UNSIGNED} values are treated; may be null if {@link BigIntUnsignedMode#PRECISE} is used.
     * @param binaryHandlingMode                 how binary columns should be treated
     * @param adjuster                           a temporal adjuster to make a database specific time before conversion
     * @param eventConvertingFailureHandlingMode how to handle conversion failures
     * @param serviceRegistry                    the service registry instance, should not be {@code null}
     */
    public VitessBinlogValueConverter(DecimalMode decimalMode, TemporalPrecisionMode temporalPrecisionMode, BigIntUnsignedMode bigIntUnsignedMode,
                                      CommonConnectorConfig.BinaryHandlingMode binaryHandlingMode, TemporalAdjuster adjuster,
                                      CommonConnectorConfig.EventConvertingFailureHandlingMode eventConvertingFailureHandlingMode,
                                      ServiceRegistry serviceRegistry) {
        super(decimalMode, temporalPrecisionMode, bigIntUnsignedMode, binaryHandlingMode, adjuster, eventConvertingFailureHandlingMode, serviceRegistry);
    }

    @Override
    protected List<String> extractEnumAndSetOptions(Column column) {
        return MySqlAntlrDdlParser.extractEnumAndSetOptions(column.enumValues());
    }

    public static VitessBinlogValueConverter getInstance(VitessConnectorConfig config) {
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode = switch (config.getBigIntUnsgnedHandlingMode()) {
            case LONG -> BigIntUnsignedMode.LONG;
            case PRECISE -> BigIntUnsignedMode.PRECISE;
            default -> null; // Handle the case where no matching mode is found
        };
        return new VitessBinlogValueConverter(
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                bigIntUnsignedMode,
                config.binaryHandlingMode(),
                ZoneOffset.UTC,
                config.getEventConvertingFailureHandlingMode(),
                config.getServiceRegistry());
    }
}
