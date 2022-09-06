package io.bf2.kafka.common;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;

public class Config {
    public static final String PREFIX = "kas.";

    public static final String POLICY_PREFIX = PREFIX + "policy.";

    // ===== start of topic policy configs definition =====
    private static final String ALTER_CONFIG_POLICY_PREFIX = Config.POLICY_PREFIX + "topic-config.";

    /**
     * Custom broker property key, used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB].
     * If this property is not specified, a default of empty list will be used in this class.
     */
    public static final String ENFORCED_VALUE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "enforced";
    public static final String ENFORCED_VALUE_CONFIGS_DOC = "This is used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB]";

    /**
     * Custom broker property key, used to specify the configs that allow to be updated. It's a comma separated list.
     * If this property is not specified, a default of empty list will be used in this class.
     */
    public static final String MUTABLE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "mutable";
    public static final String MUTABLE_CONFIGS_DOC = "This is used to specify the configs that allow to be updated. It's a comma separated list.";

    /**
     * Custom broker property key, used to specify the configs that allow values within a range with the format "configA:minA:maxA,configB:minB:maxB,...".
     * <p>
     * For example, if we want to set:
     *   1. configA value as 4 <= value <= 10
     *   2. configB value as 4 <= value
     *   3. configC value as value <= 0.8
     *
     * So, we should set the range config as: "configA:4:10,configB:4:,configC::0.8
     * <p>
     * If this property is not specified, a default of empty list will be used in this class.
     */
    public static final String RANGE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "range";
    public static final String RANGE_CONFIGS_DOC = "This is used to specify the configs that allow values within a range with the format 'configA:minA:maxA,configB:minB:maxB,....'.";

    /**
     * Feature flag broker property key to allow enabling/disabling of topic config policies. If this
     * property is not specified, a default of {@code false} will be set
     */
    public static final String TOPIC_CONFIG_POLICY_ENFORCED = ALTER_CONFIG_POLICY_PREFIX + "topic-config-policy-enforced";
    public static final String TOPIC_CONFIG_POLICY_ENFORCED_DOC = "Feature flag to allow enabling or disabling of topic config policies";

    public static final ConfigDef TOPIC_POLICY_CONFIG_DEF = new ConfigDef()
            .define(ENFORCED_VALUE_CONFIGS, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, ENFORCED_VALUE_CONFIGS_DOC)
            .define(MUTABLE_CONFIGS, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, MUTABLE_CONFIGS_DOC)
            .define(RANGE_CONFIGS, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, RANGE_CONFIGS_DOC)
            .define(TOPIC_CONFIG_POLICY_ENFORCED, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, TOPIC_CONFIG_POLICY_ENFORCED_DOC);

    // ===== end of topic policy configs definition =====

    // ===== start of partition counter configs definition =====
    private static final String CREATE_TOPIC_POLICY_PREFIX = Config.POLICY_PREFIX + "create-topic.";

    public static final int DEFAULT_MAX_PARTITIONS = -1;
    public static final int DEFAULT_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_SCHEDULE_INTERVAL_SECONDS = 15;
    public static final String DEFAULT_PRIVATE_TOPIC_PREFIX = "";
    public static final boolean DEFAULT_LIMIT_ENFORCED = false;
    /**
     * Custom broker property key, used to specify the upper limit of partitions that should be allowed
     * in the cluster. If this property is not specified, a default of {@link #DEFAULT_MAX_PARTITIONS}
     * will be used in this class.
     */
    public static final String MAX_PARTITIONS = "max.partitions";

    /**
     * Custom broker property key, used to specify the number of seconds to use as a timeout duration
     * when listing and describing topics as part of the {@link PartitionCounter#countExistingPartitions()} method. If
     * this property is not specified, a default of {@link #DEFAULT_TIMEOUT_SECONDS} will be used in
     * this class.
     */
    public static final String TIMEOUT_SECONDS = CREATE_TOPIC_POLICY_PREFIX + "partition-counter.timeout-seconds";

    /**
     * Custom broker property key, used to specify the topic prefix to match for private/internal topics
     * in the {@link PartitionCounter#countExistingPartitions()} method, where partitions from those topics will not be
     * counted. If this property is not specified, a default of {@link #DEFAULT_PRIVATE_TOPIC_PREFIX}
     * will be used in this class.
     */
    public static final String PRIVATE_TOPIC_PREFIX = CREATE_TOPIC_POLICY_PREFIX + "partition-counter.private-topic-prefix";

    /**
     * Custom broker property key, used to specify the interval (in seconds) at which to schedule
     * partition counts. If this property is not specified, a default of
     * {@link #DEFAULT_SCHEDULE_INTERVAL_SECONDS} will be used in this class.
     */
    public static final String SCHEDULE_INTERVAL_SECONDS = CREATE_TOPIC_POLICY_PREFIX + "partition-counter.schedule-interval-seconds";

    /**
     * Feature flag broker property key to allow disabling of partition limit enforcement. If this
     * property is not specified, a default of {@link #DEFAULT_LIMIT_ENFORCED} will be returned through
     * the {@link PartitionCounter#isLimitEnforced()} method.
     */
    public static final String LIMIT_ENFORCED = CREATE_TOPIC_POLICY_PREFIX + "partition-limit-enforced";

    public static final ConfigDef PARTITION_COUNTER_CONFIG_DEF = new ConfigDef()
            .define(LIMIT_ENFORCED, ConfigDef.Type.BOOLEAN, DEFAULT_LIMIT_ENFORCED, ConfigDef.Importance.MEDIUM, "Feature flag to allow enabling of partition limit enforcement")
            .define(MAX_PARTITIONS, ConfigDef.Type.INT, DEFAULT_MAX_PARTITIONS, ConfigDef.Importance.MEDIUM, "Max partitions")
            .define(PRIVATE_TOPIC_PREFIX, ConfigDef.Type.STRING, DEFAULT_PRIVATE_TOPIC_PREFIX, ConfigDef.Importance.MEDIUM, "Internal Partition Prefix")
            .define(TIMEOUT_SECONDS, ConfigDef.Type.INT, DEFAULT_TIMEOUT_SECONDS,ConfigDef.Importance.MEDIUM, "Timeout duration for listing and describing topics")
            .define(SCHEDULE_INTERVAL_SECONDS, ConfigDef.Type.INT, DEFAULT_SCHEDULE_INTERVAL_SECONDS,ConfigDef.Importance.MEDIUM, "Schedule interval for scheduled counter");
    // ===== end of partition counter configs definition =====
}
