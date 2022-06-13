package io.bf2.kafka.common;

import com.google.common.collect.Range;
import io.bf2.kafka.common.rule.ConfigRule;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.DELETE_RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.FILE_DELETE_DELAY_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.FLUSH_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.INDEX_INTERVAL_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.PREALLOCATE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.RETENTION_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_INDEX_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_JITTER_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_MS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

public class Config {
    public static final String PREFIX = "kas.";

    public static final String POLICY_PREFIX = PREFIX + "policy.";

    // ===== start of topic policy configs definition =====
    private static final String ALTER_CONFIG_POLICY_PREFIX = Config.POLICY_PREFIX + "topic-config.";

    /**
     * Custom broker property key, used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB].
     * If this property is not specified, a default of {@link #DEFAULT_ENFORCED_VALUE_SET} will be used in this class.
     */
    public static final String ENFORCED_VALUE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "enforced";
    public static final String ENFORCED_VALUE_CONFIGS_DOC = "This is used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB]";

    /**
     * Custom broker property key, used to specify the configs that allow to be updated. It's a comma separated list.
     * If this property is not specified, a default of {@link #DEFAULT_MUTABLE_CONFIG_KEYS} will be used in this class.
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
     * If this property is not specified, a default of {@link #DEFAULT_RANGE_CONFIGS} will be used in this class.
     */
    public static final String RANGE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "range";
    public static final String RANGE_CONFIGS_DOC = "This is used to specify the configs that allow values within a range with the format 'configA:minA:maxA,configB:minB:maxB,....'.";

    public static final Set<String> DEFAULT_ENFORCED_VALUE_SET = Set.of(
            COMPRESSION_TYPE_CONFIG + ":producer",
            FILE_DELETE_DELAY_MS_CONFIG + ":60000",
            FLUSH_MESSAGES_INTERVAL_CONFIG + ":9223372036854775807",
            FLUSH_MS_CONFIG + ":9223372036854775807",
            INDEX_INTERVAL_BYTES_CONFIG + ":4096",
            MIN_CLEANABLE_DIRTY_RATIO_CONFIG + ":0.5",
            PREALLOCATE_CONFIG + ":false",
            SEGMENT_JITTER_MS_CONFIG + ":0",
            UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG + ":false",
            SEGMENT_INDEX_BYTES_CONFIG + ":10485760"
    );

    public static final Set<String> DEFAULT_MUTABLE_CONFIG_KEYS = Set.of(
            CLEANUP_POLICY_CONFIG,
            COMPRESSION_TYPE_CONFIG,
            DELETE_RETENTION_MS_CONFIG,
            FILE_DELETE_DELAY_MS_CONFIG,
            FLUSH_MESSAGES_INTERVAL_CONFIG,
            FLUSH_MS_CONFIG,
            INDEX_INTERVAL_BYTES_CONFIG,
            MAX_COMPACTION_LAG_MS_CONFIG,
            MAX_MESSAGE_BYTES_CONFIG,
            MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG,
            MESSAGE_TIMESTAMP_TYPE_CONFIG,
            MIN_CLEANABLE_DIRTY_RATIO_CONFIG,
            MIN_COMPACTION_LAG_MS_CONFIG,
            PREALLOCATE_CONFIG,
            RETENTION_BYTES_CONFIG,
            RETENTION_MS_CONFIG,
            SEGMENT_BYTES_CONFIG,
            SEGMENT_INDEX_BYTES_CONFIG,
            SEGMENT_JITTER_MS_CONFIG,
            SEGMENT_MS_CONFIG,
            UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG,
            MESSAGE_DOWNCONVERSION_ENABLE_CONFIG
    );

    public static final String DEFAULT_MAX_MESSAGE_BYTES= "1048588";
    public static final String DEFAULT_MIN_SEGMENT_BYTES = "52428800";
    public static final String DEFAULT_MIN_SEGMENT_MS = "600000";
    public static final Set<String> DEFAULT_RANGE_CONFIG_SET = Set.of(
            String.format("%s::%s", MAX_MESSAGE_BYTES_CONFIG, DEFAULT_MAX_MESSAGE_BYTES),
            String.format("%s:%s:", SEGMENT_BYTES_CONFIG, DEFAULT_MIN_SEGMENT_BYTES),
            String.format("%s:%s:", SEGMENT_MS_CONFIG, DEFAULT_MIN_SEGMENT_MS));

    public static final String DEFAULT_CONFIG_VALUE_CONFIGS = String.join(",", DEFAULT_ENFORCED_VALUE_SET);
    public static final String DEFAULT_MUTABLE_CONFIGS = String.join(",", DEFAULT_MUTABLE_CONFIG_KEYS);
    public static final String DEFAULT_RANGE_CONFIGS = String.join(",", DEFAULT_RANGE_CONFIG_SET);

    public static final ConfigDef TOPIC_POLICY_CONFIG_DEF = new ConfigDef()
            .define(ENFORCED_VALUE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Importance.MEDIUM, ENFORCED_VALUE_CONFIGS_DOC)
            .define(MUTABLE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_MUTABLE_CONFIGS, ConfigDef.Importance.MEDIUM, MUTABLE_CONFIGS_DOC)
            .define(RANGE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_RANGE_CONFIGS, ConfigDef.Importance.MEDIUM, RANGE_CONFIGS_DOC);

    // ===== end of topic policy configs definition =====

    // ===== start of partition counter configs definition =====
    private static final String CREATE_TOPIC_POLICY_PREFIX = Config.POLICY_PREFIX + "create-topic.";

    public static final int DEFAULT_MAX_PARTITIONS = -1;
    public static final int DEFAULT_TIMEOUT_SECONDS = 10;
    public static final int DEFAULT_SCHEDULE_INTERVAL_SECONDS = 15;
    public static final String DEFAULT_NO_PRIVATE_TOPIC_PREFIX = "";
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
     * counted. If this property is not specified, a default of {@link #DEFAULT_NO_PRIVATE_TOPIC_PREFIX}
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
            .define(PRIVATE_TOPIC_PREFIX, ConfigDef.Type.STRING, DEFAULT_NO_PRIVATE_TOPIC_PREFIX, ConfigDef.Importance.MEDIUM, "Internal Partition Prefix")
            .define(TIMEOUT_SECONDS, ConfigDef.Type.INT, DEFAULT_TIMEOUT_SECONDS,ConfigDef.Importance.MEDIUM, "Timeout duration for listing and describing topics")
            .define(SCHEDULE_INTERVAL_SECONDS, ConfigDef.Type.INT, DEFAULT_SCHEDULE_INTERVAL_SECONDS,ConfigDef.Importance.MEDIUM, "Schedule interval for scheduled counter");
    // ===== end of partition counter configs definition =====

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of string is key1:val1,key2:val2 ....
     *
     * @param list the list with the format: key1:val1,key2:val2
     * @return  the unmodifiable map with the {key1=val1, key2=val2}
     */
    public static Map<String, String> parseListToMap(List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, String> map = new HashMap<>(list.size());

        list.stream().forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            if (delimiter == -1) {
                throw new IllegalArgumentException("The provided config is not in the correct format: config:value");
            }
            map.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });
        return Map.copyOf(map);
    }

    /**
     * This method gets comma separated values which contains key:min:max and returns a map of
     * key range pairs. the format of string is key1:min1:max1,key2:min2:max2 ....
     *
     * @param list the list with the format: key1:min1:max1,key2:min2:max2
     * @return  the unmodifiable map with the {key1=range1, key2=range2}
     */
    public static Map<String, Range<Double>> parseListToRangeMap(List<String> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Range<Double>> map = new HashMap<>(list.size());

        list.stream().forEach(s -> {
            // split "key:min:max" into [key, min, max]
            String[] parts = s.split(":", 3);
            if (parts.length != 3) {
                throw new IllegalArgumentException("The provided config is not in the correct format: config:minValue:maxValue");
            }

            String configKey = parts[0].trim();
            String min = parts[1];
            String max = parts[2];

            Double lowerBound;
            Double upperBound;
            // convert the number into double for accurate comparison
            try {
                lowerBound = min.isBlank() ? null : Double.valueOf(min);
                upperBound = max.isBlank() ? null : Double.valueOf(max);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("The provided min or max value is not a number.", e);
            }

            if (lowerBound == null && upperBound == null) {
                throw new IllegalArgumentException("The provided lower bound and upper bound value are empty.");
            } else if (lowerBound == null) {
                map.put(configKey, Range.atMost(upperBound));
            } else if (upperBound == null) {
                map.put(configKey, Range.atLeast(lowerBound));
            } else {
                map.put(configKey, Range.closed(lowerBound, upperBound));
            }
        });
        return Map.copyOf(map);
    }
}
