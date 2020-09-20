/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.vitess.connection.VgtidReader;
import io.debezium.function.Predicates;
import io.debezium.relational.ColumnId;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;

/**
 * Vitess connector configuration, including its specific configurations and the common
 * configurations from Debezium.
 */
public class VitessConnectorConfig extends RelationalDatabaseConnectorConfig {

    public static final String VITESS_CONFIG_GROUP_PREFIX = "vitess.";

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
            .withValidation(Field::isRequired)
            .withDescription(
                    "Single shard of which keyspace to read data from."
                            + "E.p. \"0\" for an unsharded keyspace. "
                            + "Or \"-80\" for the -80 shard of the sharded keyspace.");

    public static final Field VTGATE_HOST = Field.create(VITESS_CONFIG_GROUP_PREFIX + "vtgate.host")
            .withDisplayName("VTGate server hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("VTGate gRPC server host name. E.p. \"localhost\".");

    public static final Field VTGATE_PORT = Field.create(VITESS_CONFIG_GROUP_PREFIX + "vtgate.port")
            .withDisplayName("VTGate server port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("VTGate gRPC server port. E.p. \"15991\".");

    public static final Field VTCTLD_HOST = Field.create(VITESS_CONFIG_GROUP_PREFIX + "vtctld.host")
            .withDisplayName("VTGate server hostname")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("VTCtld gRPC server host name. E.p. \"localhost\".");

    public static final Field VTCTLD_PORT = Field.create(VITESS_CONFIG_GROUP_PREFIX + "vtctld.port")
            .withDisplayName("VTGate server port")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(Field::isRequired)
            .withDescription("VTCtld gRPC server port. E.p. \"15999\".");

    public static final Field TABLET_TYPE = Field.create(VITESS_CONFIG_GROUP_PREFIX + "tablet.type")
            .withDisplayName("Tablet type to get data-changes")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDefault(VgtidReader.TabletType.MASTER.name())
            .withDescription(
                    "Tablet type used to get latest vgtid from Vtctld and get data-changes from Vtgate."
                            + " Value can be MASTER, REPLICA, and RDONLY.");

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
            .type(KEYSPACE, SHARD, VTGATE_HOST, VTGATE_PORT, VTCTLD_HOST, VTCTLD_PORT, TABLET_TYPE)
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
        super(config, config.getString(SERVER_NAME), null, x -> x.schema() + "." + x.table(), -1);
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

    private static ColumnNameFilter getColumnNameFilter(String excludedColumnPatterns) {
        return new ColumnNameFilter() {

            Predicate<ColumnId> delegate = Predicates.excludes(excludedColumnPatterns, ColumnId::toString);

            @Override
            public boolean matches(
                                   String catalogName, String schemaName, String tableName, String columnName) {
                // ignore database name as it's not relevant here
                return delegate.test(new ColumnId(new TableId(null, schemaName, tableName), columnName));
            }
        };
    }

    public String getKeyspace() {
        return getConfig().getString(KEYSPACE);
    }

    public String getShard() {
        return getConfig().getString(SHARD);
    }

    public String getVtgateHost() {
        return getConfig().getString(VTGATE_HOST);
    }

    public int getVtgatePort() {
        return getConfig().getInteger(VTGATE_PORT);
    }

    public String getVtctldHost() {
        return getConfig().getString(VTCTLD_HOST);
    }

    public int getVtctldPort() {
        return getConfig().getInteger(VTCTLD_PORT);
    }

    public String getTabletType() {
        return getConfig().getString(TABLET_TYPE);
    }

    public boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }
}
