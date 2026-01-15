/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.config.Field.ValidationOutput;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.vitess.connection.VitessTabletType;
import io.debezium.connector.vitess.pipeline.txmetadata.ShardEpochMap;
import io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.GrpcUtil;

/**
 * Vitess connector configuration, including its specific configurations and the common
 * configurations from Debezium.
 */
public class VitessConnectorConfig extends RelationalDatabaseConnectorConfig {

    public static final String CSV_DELIMITER = ",";

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessConnectorConfig.class);

    private static final String VITESS_CONFIG_GROUP_PREFIX = "vitess.";
    private static final int DEFAULT_VTGATE_PORT = 15_991;
    public static final long DEFAULT_CONNECTOR_GENERATION = 0L;

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform an initial snapshot when starting, if it does not detect a value in its offsets topic.
         */
        INITIAL("initial"),

        /**
         * Never perform an initial snapshot and only receive new data changes.
         */
        NEVER("never");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();

            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }

            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);

            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }

            return mode;
        }
    }

    public enum BigIntUnsignedHandlingMode implements EnumeratedValue {
        /**
         * Represent {@code BIGINT UNSIGNED} values as string.
         */
        STRING("string"),

        /**
         * Represent {@code BIGINT UNSIGNED} values as precise {@link BigDecimal} values, which are
         * represented in change events in a binary form. This is precise but difficult to use.
         */
        PRECISE("precise"),

        /**
         * Represent {@code BIGINT UNSIGNED} values as {@code long} values. This may not be precise
         * when the value is over 9,223,372,036,854,775,807, but it might be how other data
         * system (e.g. Hudi) was mapping big int unsigned to.
         */
        LONG("long");

        private final String value;

        BigIntUnsignedHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static BigIntUnsignedHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (BigIntUnsignedHandlingMode option : BigIntUnsignedHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static BigIntUnsignedHandlingMode parse(String value, String defaultValue) {
            BigIntUnsignedHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

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

    public static final Field STREAM_KEYSPACE_HEARTBEATS = Field.create(VITESS_CONFIG_GROUP_PREFIX + "stream.keyspace.heartbeats")
            .withDisplayName("stream.keyspace.heartbeats")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Streams the events for the vitess heartbeat tables. Heartbeats must also be enabled on the Vitess tablets. " +
                    "If a Debezium table include list is configured, the heartbeat table should be specified there, the format is `<keyspace>.heartbeat)`");

    public static final Field EXCLUDE_KEYSPACE_FROM_TABLE_NAME = Field.create(VITESS_CONFIG_GROUP_PREFIX + "exclude.keyspace.from.table.name")
            .withDisplayName("exclude.keyspace.from.table.name")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Excludes the keyspace from the table name which boosts the VTGate performance significantly" +
                    "(avoids unnecessarily copying each event before sending to Debezium)." +
                    "Only safe to do for Debezium clients streaming from one keyspace (currently the only supported mode of operation).");

    public static final Field SHARD = Field.create(VITESS_CONFIG_GROUP_PREFIX + "shard")
            .withDisplayName("Shard")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription(
                    "Shard(s) of which keyspace to read data from."
                            + "E.g. \"0\" for an unsharded keyspace. "
                            + "Or \"-80\" for the -80 shard of the sharded keyspace."
                            + "Or \"-4000,4000-8000\" for two of the four shards of a sharded keyspace.");

    public static final Field VGTID = Field.create(VITESS_CONFIG_GROUP_PREFIX + "vgtid")
            .withDisplayName("vgtid")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withDefault(Vgtid.CURRENT_GTID)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(VitessConnectorConfig::validateVgtids)
            .withDescription(
                    "VGTID from where to start reading from for the given shard(s)."
                            + " It has to be set together with vitess.shard."
                            + " If not configured, the connector streams changes from the latest position for the given shard(s)."
                            + " If snapshot.mode is INITIAL (default), the connector starts copying the tables for the given shard(s) first regardless of gtid value.");

    public static final Field SHARD_EPOCH_MAP = Field.create(VITESS_CONFIG_GROUP_PREFIX + "shard.epoch.map")
            .withDisplayName("shard.epoch.map")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withDefault("")
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(VitessConnectorConfig::validateShardEpochMap)
            .withDescription(
                    "ShardEpochMap to use for the initial epoch values for the given shards. If not configured the connector streams changes" +
                            "from a default value of 0.");

    public static final Field GTID = Field.create(VITESS_CONFIG_GROUP_PREFIX + "gtid")
            .withDisplayName("gtid")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withDefault(Vgtid.CURRENT_GTID)
            .withImportance(ConfigDef.Importance.HIGH)
            .withValidation(VitessConnectorConfig::validateVgtids)
            .withDescription(
                    "This option is deprecated use vitess.vgtid instead."
                            + "VGTID from where to start reading from for the given shard(s)."
                            + " It has to be set together with vitess.shard."
                            + " If not configured, the connector streams changes from the latest position for the given shard(s)."
                            + " If snapshot.mode is INITIAL (default), the connector starts copying the tables for the given shard(s) first regardless of gtid value.");

    public static final Field EXCLUDE_EMPTY_SHARDS = Field.create(VITESS_CONFIG_GROUP_PREFIX + "exclude.empty.shards")
            .withDisplayName("exclude.empty.shards")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withDefault(false)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Auto-detects and excludes empty shards from queries & shard lists used for VStreams");

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

    public static final Field INHERIT_EPOCH = Field.create(VITESS_CONFIG_GROUP_PREFIX + "inherit.epoch")
            .withDisplayName("Inherit epoch")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withValidation(VitessConnectorConfig::validateInheritEpoch)
            .withDescription("Controls whether the epochs of a new shard after a re-shard operation inherits epochs from its parent shards");

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

    public static final Field GRPC_MAX_INBOUND_MESSAGE_SIZE = Field.create(VITESS_CONFIG_GROUP_PREFIX + "grpc.max_inbound_message_size")
            .withDisplayName("VStream gRPC maxInboundMessageSize")
            .withType(Type.INT)
            .withWidth(Width.SHORT)
            .withDefault(4_194_304)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(Field::isInteger)
            .withDescription("Specify the maximum message size in bytes allowed to be received on the channel.");

    public static final Field GRPC_DEFAULT_LOAD_BALANCING_POLICY = Field.create(VITESS_CONFIG_GROUP_PREFIX + "grpc.default.load.balancing.policy")
            .withDisplayName("VStream gRPC default load balancing policy")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withDefault(GrpcUtil.DEFAULT_LB_POLICY)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withValidation(VitessConnectorConfig::validateLoadBalancingPolicy)
            .withDescription("Specify the default load balancing policy used to connect to Vitess, e.g., 'pick_first', 'round_robin'");

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

    // Needed for backward compatibility, otherwise all upgraded connectors will start publishing schema change events
    // by default.
    public static final Field INCLUDE_SCHEMA_CHANGES = Field.create("include.schema.changes")
            .withDisplayName("Include database schema changes")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 0))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Whether the connector should publish changes in the database schema to a Kafka topic with "
                    + "the same name as the database server ID. Each schema change will be recorded using a key that "
                    + "contains the database name and whose value include logical description of the new schema and optionally the DDL statement(s). "
                    + "The default is 'true'. This is independent of how the connector internally records database schema history.")
            .withDefault(false);

    public static final Field OFFSET_STORAGE_PER_TASK = Field.create(VITESS_CONFIG_GROUP_PREFIX + "offset.storage.per.task")
            .withDisplayName("Store offsets per task")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription(
                    "Whether to store the offsets in Kafka's offset storage topic by task id. "
                            + "You must set offset.storage.per.task to true if tasks.max > 1"
                            + "'false' (the default) offsets are stored as a single unit under the database name. "
                            + "'true' stores the offsets per task id");

    public static final Field OFFSET_STORAGE_TASK_KEY_GEN = Field.create(VITESS_CONFIG_GROUP_PREFIX + "offset.storage.task.key.gen")
            .withDisplayName("Offset storage task key generation number")
            .withType(Type.INT)
            .withDefault(-1)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription(
                    "Offset storage task key generation number. "
                            + "The partition key in the offset storage will be in the form of <taskId>_<numTasks>_<gen>. "
                            + "You will increase the <gen> number when the parallelism of your tasks are changing. \n"
                            + "This will make each generation of task parallelism leaves different sets of partition keys in the offset storage. "
                            + "E.g. you were using 2 tasks for the connector previously and now you want to use 4 tasks. "
                            + "Previously you might specify <gen> as 1 and now you will specify <gen> as 2. "
                            + "Previously the partition key in the offset storage will be task0_2_1, task1_2_1, "
                            + "And now the partition key in the offset storage will be task0_4_2, task1_4_2, task2_4_2, task3_4_2. \n"
                            + "Note that for generation number lineage tracking purpose, generation number starts with 0.  "
                            + "If your installation previously did not use offset.storage.per.task, the offset storage "
                            + " key will be in the form of server=db_1, this will implicitly be treated as generation 0. "
                            + "And when you switch to use offset.storage.per.task mode, you should specify task.key.gen=1 "
                            + "so we can establish the offset generation lineage for offset migration. \n"
                            + "If your installation starts with offset.storage.per.task mode upfront (which means you don't have "
                            + "any previous key in offset storage, you should start with specifying task.key.gen = 0 explicitly "
                            + "So we know this run is the origin.");

    public static final Field PREV_NUM_TASKS = Field.create(VITESS_CONFIG_GROUP_PREFIX + "prev.num.tasks")
            .withDisplayName("Previous number of tasks")
            .withType(Type.INT)
            .withDefault(-1)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription(
                    "Previous number of tasks used for the previous generation of task parallelism. \n"
                            + "This param is only used when your tasks parallelism is changing and "
                            + "We will use prev.num.tasks to fetch the existing offset from offset storage associated "
                            + "with your previous run to keep the offset progression continuously. \n"
                            + "E.g. Previously you were using 2 tasks for the connector and offset.storage.task.key.gen = 1, "
                            + "The partition keys in the offset storage were task0_2_1, task1_2_1, \n"
                            + "Now you want to use 4 tasks. You will specify prev.num.tasks = 2 and offset.storage.task.key.gen = 2, "
                            + "We will use this information to fetch the offsets through partition keys from previous run, "
                            + "Previous run's partition keys will be calculated using <taskId>_<prev.num.task>_<offset.storage.task.key.gen - 1>. \n"
                            + "Note this param is only used once the first time when we detect task parallelism change. "
                            + "Once we persist the new offsets in offset storage using new partition key "
                            + "based on current <numTasks> and <gen>, we will no longer read prev.num.tasks param");

    public static final Field CONNECTOR_GENERATION = Field.create(VITESS_CONFIG_GROUP_PREFIX + "connector.generation")
            .withDisplayName("Connector Generation")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDefault(DEFAULT_CONNECTOR_GENERATION)
            .withDescription("Generation number for transaction ordering semantics. " +
                    "Increment this when making changes that affect transaction ordering. " +
                    "The epoch will be automatically incremented when the generation increases. " +
                    "This setting only takes effect when transaction.metadata.factory is set to " +
                    "io.debezium.connector.vitess.pipeline.txmetadata.VitessOrderedTransactionMetadataFactory.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'initial' (the default) to specify the connector should always perform an initial sync when required; "
                    + "'never' to specify the connector should never perform an initial sync ");

    public static final Field BIGINT_UNSIGNED_HANDLING_MODE = Field.create("bigint.unsigned.handling.mode")
            .withDisplayName("BIGINT UNSIGNED Handling")
            .withEnum(BigIntUnsignedHandlingMode.class, BigIntUnsignedHandlingMode.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 1))
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("Specify how BIGINT UNSIGNED columns should be represented in change events, including: "
                    + "'string' (the default) represent values using Java's 'string'; "
                    + "'precise' represents values as precise (Java's 'BigDecimal') values;"
                    + "'long' represents values using Java's 'long', which may not offer the precision but will be far easier to use in consumers.");

    public static final Field OVERRIDE_DATETIME_TO_NULLABLE = Field.create("override.datetime.to.nullable")
            .withDisplayName("Override datetime to nullable")
            .withType(Type.BOOLEAN)
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(ConfigDef.Importance.MEDIUM)
            .withDescription("If enabled, makes all date & datetime columns nullable. Date & datetime types are incapable of representing zero-date values i.e., with" +
                    "month or day set to zero, e.g., 0000-00-00 or 0000-00-00 00:00:00. By overriding to nullable, the null value can be set in place" +
                    "of these zero-value temporal types. If disabled, zero-dates are converted to the epoch value (and cannot be differentiated from " +
                    "an actual epoch value)");

    public static final Field TIME_PRECISION_MODE = RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE
            .withEnum(TemporalPrecisionMode.class, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 26))
            .withValidation(VitessConnectorConfig::validateTimePrecisionMode)
            .withDescription("Time, date and timestamps can be represented with different kinds of precisions, including: "
                    + "'adaptive_time_microseconds': the precision of date and timestamp values is based the database column's precision; but time fields always use microseconds precision; "
                    + "'connect': always represents time, date and timestamp values using Kafka Connect's built-in representations for Time, Date, and Timestamp, "
                    + "which uses millisecond precision regardless of the database columns' precision.");

    private static int validateTimePrecisionMode(Configuration config, Field field, ValidationOutput problems) {
        if (config.hasKey(TIME_PRECISION_MODE.name())) {
            final String timePrecisionMode = config.getString(TIME_PRECISION_MODE.name());
            if (TemporalPrecisionMode.ADAPTIVE.getValue().equals(timePrecisionMode)) {
                // this is a problem
                problems.accept(TIME_PRECISION_MODE, timePrecisionMode, "The 'adaptive' time.precision.mode is no longer supported");
                return 1;
            }
        }

        // Everything checks out ok.
        return 0;
    }

    private static int validateLoadBalancingPolicy(Configuration config, Field field, ValidationOutput problems) {
        if (config.hasKey(GRPC_DEFAULT_LOAD_BALANCING_POLICY)) {
            final String policy = config.getString(GRPC_DEFAULT_LOAD_BALANCING_POLICY.name());
            LoadBalancerProvider provider = LoadBalancerRegistry.getDefaultRegistry().getProvider(policy);
            if (provider == null) {
                problems.accept(GRPC_DEFAULT_LOAD_BALANCING_POLICY, policy, "No load balancer provider for policy: " + policy);
                return 1;
            }
        }
        return 0;
    }

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(VitessSourceInfoStructMaker.class.getName());

    protected static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION
            .edit()
            .name("Vitess")
            .type(
                    KEYSPACE,
                    SHARD,
                    VGTID,
                    GTID,
                    VTGATE_HOST,
                    VTGATE_PORT,
                    VTGATE_USER,
                    VTGATE_PASSWORD,
                    TABLET_TYPE,
                    STOP_ON_RESHARD_FLAG,
                    KEEPALIVE_INTERVAL_MS,
                    GRPC_HEADERS,
                    GRPC_MAX_INBOUND_MESSAGE_SIZE,
                    BINARY_HANDLING_MODE,
                    SCHEMA_NAME_ADJUSTMENT_MODE,
                    OFFSET_STORAGE_PER_TASK,
                    OVERRIDE_DATETIME_TO_NULLABLE,
                    OFFSET_STORAGE_TASK_KEY_GEN,
                    PREV_NUM_TASKS,
                    CONNECTOR_GENERATION,
                    STREAM_KEYSPACE_HEARTBEATS,
                    EXCLUDE_KEYSPACE_FROM_TABLE_NAME,
                    EXCLUDE_EMPTY_SHARDS)
            .events(
                    INCLUDE_UNKNOWN_DATATYPES,
                    SOURCE_INFO_STRUCT_MAKER)
            .connector(
                    SNAPSHOT_MODE,
                    BIGINT_UNSIGNED_HANDLING_MODE,
                    TIME_PRECISION_MODE)
            .excluding(
                    SCHEMA_EXCLUDE_LIST,
                    SCHEMA_INCLUDE_LIST,
                    RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE)
            .create();

    // tasks.max is defined in org.apache.kafka.connect.runtime.ConnectorConfig
    // We copy the definition here instead of importing the class from connect.runtime package
    protected static final String TASKS_MAX_CONFIG = "tasks.max";

    // The vitess.task.key config, the value is in the form of <task-id>_<num-tasks>_<gen>,
    // VitessConnector will populate the value of this param and pass on to VitessConnectorTask
    protected static final String VITESS_TASK_KEY_CONFIG = "vitess.task.key";

    // The total number of tasks that will be used by the vitess connector, which is
    // deterimined dynamically in VitessConnector, and can be different from the
    // tasks.max config (e.g., tasks.max > number of shards)
    protected static final String VITESS_TOTAL_TASKS_CONFIG = "vitess.total.tasks";

    // The vitess.task.shards config, the value is a comma separated vitess shard names
    // VitessConnector will populate the value of this param and pass on to VitessConnectorTask
    protected static final String VITESS_TASK_SHARDS_CONFIG = "vitess.task.shards";

    // The vitess.task.shard.to.epoch config, the value is a JSON map with vitess shard names mapping
    // to epoch values. The vitess connector will populate the value of this param and pass on to
    // VitessConnectorTask when the TransactionContextFactory is set to VitessOrderedTransactionContext
    public static final String VITESS_TASK_SHARD_EPOCH_MAP_CONFIG = "vitess.task.shard.epoch.map";

    // The vgtid assigned to the given task in the json format, this is the same format as we would see
    // in the Kafka offset storage.
    // e.g. [{\"keyspace\":\"ks\",\"shard\":\"-80\",\"gtid\":\"MySQL56/0001:1-114\"},
    // {\"keyspace\":\"ks\",\"shard\":\"80-\",\"gtid\":\"MySQL56/0002:1-122\"}]
    public static final String VITESS_TASK_VGTID_CONFIG = "vitess.task.vgtid";

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    public VitessConnectorConfig(Configuration config) {
        super(
                config,
                null, x -> x.schema() + "." + x.table(),
                -1,
                ColumnFilterMode.SCHEMA,
                true);
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
    public boolean isSchemaChangesHistoryEnabled() {
        return getConfig().getBoolean(INCLUDE_SCHEMA_CHANGES);
    }

    @Override
    public TemporalPrecisionMode getTemporalPrecisionMode() {
        return TemporalPrecisionMode.parse(getConfig().getString(TIME_PRECISION_MODE));
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        // Assume V2 is used because it is the default version
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    public String getKeyspace() {
        return getConfig().getString(KEYSPACE);
    }

    public List<String> getShard() {
        return getConfig().getStrings(SHARD, CSV_DELIMITER);
    }

    public String getVgtid() {
        if (getSnapshotMode() == SnapshotMode.INITIAL) {
            return Vgtid.EMPTY_GTID;
        }
        String value = getConfig().getString(VGTID);
        return (value != null && !VGTID.defaultValueAsString().equals(value)) ? value : Vgtid.CURRENT_GTID;
    }

    public String getShardEpochMap() {
        return getConfig().getString(SHARD_EPOCH_MAP);
    }

    public boolean excludeEmptyShards() {
        return getConfig().getBoolean(EXCLUDE_EMPTY_SHARDS);
    }

    public boolean getStreamKeyspaceHeartbeats() {
        return getConfig().getBoolean(STREAM_KEYSPACE_HEARTBEATS);
    }

    public boolean getExcludeKeyspaceFromTableName() {
        return getConfig().getBoolean(EXCLUDE_KEYSPACE_FROM_TABLE_NAME);
    }

    private static int validateVgtids(Configuration config, Field field, ValidationOutput problems) {
        // Get the GTID as a string so that the default value is used if GTID is not set
        String vgtidString = config.getString(field);
        if (vgtidString.equals(Vgtid.CURRENT_GTID) || vgtidString.equals(Vgtid.EMPTY_GTID)) {
            return 0;
        }
        if (field.equals(VitessConnectorConfig.GTID)) {
            LOGGER.warn("Field {} is deprecated, use {} instead", field, VitessConnectorConfig.VGTID);
        }
        List<String> shards = config.getStrings(SHARD, CSV_DELIMITER);
        Vgtid vgtid = Vgtid.of(vgtidString);
        if (shards == null && vgtid.getShardGtids() != null) {
            problems.accept(field, vgtid, "If GTIDs are specified, there must be shards specified");
            return 1;
        }
        if (shards != null && shards.size() != vgtid.getShardGtids().size()) {
            problems.accept(field, vgtid, "If GTIDs are specified must be specified for all shards");
            return 1;
        }
        Set<String> configShards = new HashSet(shards);
        Set<String> vgtidConfigShards = vgtid.getShardGtids().stream().map(shardGtid -> shardGtid.getShard()).collect(Collectors.toSet());
        if (!configShards.equals(vgtidConfigShards)) {
            problems.accept(field, vgtid, "If GTIDs are specified must be specified for matching shards");
            return 1;
        }
        return 0;
    }

    private static int validateShardEpochMap(Configuration config, Field field, ValidationOutput problems) {
        // Get the GTID as a string so that the default value is used if GTID is not set
        String shardEpochMapString = config.getString(field);
        if (shardEpochMapString.isEmpty()) {
            return 0;
        }
        try {
            ShardEpochMap shardEpochMap = ShardEpochMap.of(shardEpochMapString);
        }
        catch (IllegalStateException e) {
            problems.accept(field, shardEpochMapString, "Shard epoch map string improperly formatted");
            return 1;
        }
        return 0;
    }

    private static int validateInheritEpoch(Configuration config, Field field, ValidationOutput problems) {
        Boolean inheritEpoch = config.getBoolean(field);
        String factory = config.getString(CommonConnectorConfig.TRANSACTION_METADATA_FACTORY);
        if (inheritEpoch && !factory.equals(VitessOrderedTransactionMetadataFactory.class.getName())) {
            problems.accept(field, inheritEpoch, "Inherit epoch cannot be enabled without VitessOrderedTransactionMetadataFactory");
        }
        return 0;
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

    public boolean getInheritEpoch() {
        return getConfig().getBoolean(INHERIT_EPOCH);
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

        for (String header : grpcHeaders.split(CSV_DELIMITER)) {
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

    public int getGrpcMaxInboundMessageSize() {
        return getConfig().getInteger(GRPC_MAX_INBOUND_MESSAGE_SIZE);
    }

    public String getGrpcDefaultLoadBalancingPolicy() {
        return getConfig().getString(GRPC_DEFAULT_LOAD_BALANCING_POLICY);
    }

    public boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }

    public boolean offsetStoragePerTask() {
        return getConfig().getBoolean(OFFSET_STORAGE_PER_TASK);
    }

    public boolean overrideDatetimeToNullable() {
        return getConfig().getBoolean(OVERRIDE_DATETIME_TO_NULLABLE);
    }

    public int getOffsetStorageTaskKeyGen() {
        return getConfig().getInteger(OFFSET_STORAGE_TASK_KEY_GEN);
    }

    public int getPrevNumTasks() {
        return getConfig().getInteger(PREV_NUM_TASKS);
    }

    public long getConnectorGeneration() {
        return getConfig().getLong(CONNECTOR_GENERATION);
    }

    public String getVitessTaskKey() {
        return getConfig().getString(VITESS_TASK_KEY_CONFIG);
    }

    public int getVitessTotalTasksConfig() {
        return getConfig().getInteger(VITESS_TOTAL_TASKS_CONFIG);
    }

    public List<String> getVitessTaskKeyShards() {
        return getConfig().getStrings(VITESS_TASK_SHARDS_CONFIG, CSV_DELIMITER);
    }

    public ShardEpochMap getVitessTaskShardEpochMap() {
        return ShardEpochMap.of(getConfig().getString(VITESS_TASK_SHARD_EPOCH_MAP_CONFIG));
    }

    public Vgtid getVitessTaskVgtid() {
        String vgtidStr = getConfig().getString(VITESS_TASK_VGTID_CONFIG);
        return vgtidStr == null ? null : Vgtid.of(vgtidStr);
    }

    public SnapshotMode getSnapshotMode() {
        return SnapshotMode.parse(getConfig().getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
    }

    @Override
    public Optional<EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    public BigIntUnsignedHandlingMode getBigIntUnsgnedHandlingMode() {
        return BigIntUnsignedHandlingMode.parse(getConfig().getString(BIGINT_UNSIGNED_HANDLING_MODE),
                BIGINT_UNSIGNED_HANDLING_MODE.defaultValueAsString());
    }
}
