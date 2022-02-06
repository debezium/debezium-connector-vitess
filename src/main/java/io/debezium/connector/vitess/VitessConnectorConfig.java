/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.vitess.connection.VitessTabletType;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Vitess connector configuration, including its specific configurations and the common
 * configurations from Debezium.
 */
public class VitessConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorConfig.class);

    private static final String VITESS_CONFIG_GROUP_PREFIX = "vitess.";
    private static final int DEFAULT_VTGATE_PORT = 15_991;

    public static final Field VTGATE_HOST = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME)
            .withDisplayName("Vitess database hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("Resolvable hostname or IP address of the Vitess VTGate gRPC server.");

    public static final Field VTGATE_PORT = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PORT)
            .withDisplayName("Vitess database port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(DEFAULT_VTGATE_PORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isInteger)
            .withDescription("Port of the Vitess VTGate gRPC server.");

    public static final Field VTGATE_USER = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.USER)
            .withDisplayName("User")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the user to be used when connecting the Vitess VTGate gRPC server.");

    public static final Field VTGATE_PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.PASSWORD)
            .withDisplayName("Password")
            .withType(Type.PASSWORD)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Password of the user to be used when connecting the Vitess VTGate gRPC server.");

    public static final Field KEYSPACE = Field.create(VITESS_CONFIG_GROUP_PREFIX + "keyspace")
            .withDisplayName("Keyspace")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription(
                    "Vitess Keyspace is equivalent to MySQL database (a.k.a schema). E.p. \"commerce\"");

    public static final Field SHARD = Field.create(VITESS_CONFIG_GROUP_PREFIX + "shard")
            .withDisplayName("Shard")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Single shard of which keyspace to read data from."
                            + "E.p. \"0\" for an unsharded keyspace. "
                            + "Or \"-80\" for the -80 shard of the sharded keyspace.");

    public static final Field GTID = Field.create(VITESS_CONFIG_GROUP_PREFIX + "gtid")
            .withDisplayName("gtid")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withDefault("current")
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Single GTID from where to start reading from for a given shard."
                            + " It has to be set together with vitess.shard");

    public static final Field TABLET_TYPE = Field.create(VITESS_CONFIG_GROUP_PREFIX + "tablet.type")
            .withDisplayName("Tablet type to get data-changes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(VitessTabletType.MASTER.name())
            .withDescription(
                    "Tablet type used to get latest vgtid from Vtctld and get data-changes from Vtgate."
                            + " Value can be MASTER, REPLICA, and RDONLY.");

    public static final Field STOP_ON_RESHARD_FLAG = Field.create(VITESS_CONFIG_GROUP_PREFIX + "stop_on_reshard")
            .withDisplayName("VStream flag stop_on_reshard")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Control StopOnReshard VStream flag."
                    + " If set true, the old VStream will be stopped after a reshard operation.");

    public static final Field KEEPALIVE_INTERVAL_MS = Field.create(VITESS_CONFIG_GROUP_PREFIX + "keepalive.interval.ms")
            .withDisplayName("VStream gRPC keepalive interval (ms)")
            .withType(Type.LONG)
            .withDefault(Long.MAX_VALUE)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Control the interval between periodic gPRC keepalive pings for VStream." +
                    " Defaults to Long.MAX_VALUE (disabled).");

    public static final Field GRPC_HEADERS = Field.create(VITESS_CONFIG_GROUP_PREFIX + "grpc.headers")
            .withDisplayName("VStream gRPC headers")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify a comma-separated list of gRPC headers." +
                    " Defaults to empty");

    public static final Field INCLUDE_UNKNOWN_DATATYPES = Field.create("include.unknown.datatypes")
            .withDisplayName("Include unknown datatypes")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription(
                    "Specify whether the fields of data type not supported by Debezium should be processed:"
                            + "'false' (the default) omits the fields; "
                            + "'true' converts the field into an implementation dependent binary representation.");

    protected static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
            .edit()
            .name("Vitess")
            .type(
                    KEYSPACE,
                    SHARD,
                    GTID,
                    VTGATE_HOST,
                    VTGATE_PORT,
                    VTGATE_USER,
                    VTGATE_PASSWORD,
                    TABLET_TYPE,
                    STOP_ON_RESHARD_FLAG,
                    KEEPALIVE_INTERVAL_MS,
                    GRPC_HEADERS,
                    BINARY_HANDLING_MODE)
            .events(INCLUDE_UNKNOWN_DATATYPES)
            .excluding(SCHEMA_EXCLUDE_LIST, SCHEMA_INCLUDE_LIST)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public VitessConnectorConfig(Configuration config) {
        super(config, config.getString(SERVER_NAME), null, x -> x.schema() + "." + x.table(), -1, ColumnFilterMode.CATALOG);
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        // Assume V2 is used because it is the default version
        return new VitessSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    public String getKeyspace() {
        return getConfig().getString(KEYSPACE);
    }

    public String getShard() {
        return getConfig().getString(SHARD);
    }

    public String getGtid() {
        return getConfig().getString(GTID);
    }

    public String getVtgateHost() {
        return getConfig().getString(VTGATE_HOST);
    }

    public int getVtgatePort() {
        return getConfig().getInteger(VTGATE_PORT);
    }

    public String getVtgateUsername() {
        return getConfig().getString(VTGATE_USER);
    }

    public String getVtgatePassword() {
        return getConfig().getString(VTGATE_PASSWORD);
    }

    public String getTabletType() {
        return getConfig().getString(TABLET_TYPE);
    }

    public boolean getStopOnReshard() {
        return getConfig().getBoolean(STOP_ON_RESHARD_FLAG);
    }

    public Duration getKeepaliveInterval() {
        return getConfig().getDuration(KEEPALIVE_INTERVAL_MS, ChronoUnit.MILLIS);
    }

    public Map<String, String> getGrpcHeaders() {
        String grpcHeaders = getConfig().getString(GRPC_HEADERS);

        if (grpcHeaders == null) {
            return Collections.emptyMap();
        }

        Map<String, String> grpcHeadersMap = new HashMap<>();

        for (String header : grpcHeaders.split(",")) {
            String[] keyAndValue = header.split(":");
            if (keyAndValue.length == 2) {
                grpcHeadersMap.put(keyAndValue[0], keyAndValue[1]);
            }
            else {
                LOGGER.warn("The following gRPC header is invalid: {}", header);
            }
        }

        return Collections.unmodifiableMap(grpcHeadersMap);
    }

    public boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }

}
