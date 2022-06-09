package io.bf2.kafka.common.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.bf2.kafka.common.Config;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.bf2.kafka.common.Utils.parseListToMap;
import static io.bf2.kafka.common.Utils.parseListToRangeMap;
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

/**
 * A class that parses the user provided rule configs to create all rules, and validates the provided configs.
 */
public class ConfigRules {
    public static final String ALTER_CONFIG_POLICY_PREFIX = Config.POLICY_PREFIX + "topic-config.";

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
            UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG + ":false"
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
    public static final Set<String> DEFAULT_RANGE_CONFIG_SET = Set.of(
            String.format("%s::%s", MAX_MESSAGE_BYTES_CONFIG, DEFAULT_MAX_MESSAGE_BYTES),
            String.format("%s:%s:", SEGMENT_BYTES_CONFIG, DEFAULT_MIN_SEGMENT_BYTES));

    public static final String DEFAULT_CONFIG_VALUE_CONFIGS = String.join(",", DEFAULT_ENFORCED_VALUE_SET);
    public static final String DEFAULT_MUTABLE_CONFIGS = String.join(",", DEFAULT_MUTABLE_CONFIG_KEYS);
    public static final String DEFAULT_RANGE_CONFIGS = String.join(",", DEFAULT_RANGE_CONFIG_SET);

    public final Set<ConfigRule> configRules;

    public static final ConfigDef configDef = new ConfigDef()
            .define(ENFORCED_VALUE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Importance.MEDIUM, ENFORCED_VALUE_CONFIGS_DOC)
            .define(MUTABLE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_MUTABLE_CONFIGS, ConfigDef.Importance.MEDIUM, MUTABLE_CONFIGS_DOC)
            .define(RANGE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_RANGE_CONFIGS, ConfigDef.Importance.MEDIUM, RANGE_CONFIGS_DOC);

    private final Map<String, String> enforcedConfigs;
    private final Set<String> mutableConfigs;
    private final Map<String, Range> rangeConfigs;

    public ConfigRules(Map<String, ?> configs) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, configs);

        enforcedConfigs = parseListToMap(parsedConfig.getList(ENFORCED_VALUE_CONFIGS));
        rangeConfigs = parseListToRangeMap(parsedConfig.getList(RANGE_CONFIGS));

        // mutable configs should be the union of all config keys
        mutableConfigs = ImmutableSet.<String>builder()
                .addAll(Set.copyOf(parsedConfig.getList(MUTABLE_CONFIGS)))
                .addAll(enforcedConfigs.keySet())
                .addAll(rangeConfigs.keySet())
                .build();

        configRules = Set.of(
                new EnforcedValueRule(enforcedConfigs),
                new ImmutableRule(mutableConfigs),
                new RangeRule(rangeConfigs));
    }

    public Map<String, String> getEnforcedConfigs() {
        return enforcedConfigs;
    }

    public Map<String, Range> getRangeConfigs() {
        return rangeConfigs;
    }

    public Set<String> getMutableConfigs() {
        return mutableConfigs;
    }

    public void validateTopicConfigs(String topic, Map<String, String> configs) {
        Set<String> invalidConfigMsgs = new HashSet<>();
        for (Map.Entry<String, String> entry: configs.entrySet()) {
            for (ConfigRule rule : configRules) {
                rule.validate(entry.getKey(), entry.getValue()).ifPresent((invalidConfigMsgs::add));
            }
        }

        if (!invalidConfigMsgs.isEmpty()) {
            throw new PolicyViolationException(
                    String.format("Invalid config specified for topic %s. The violating configs are: %s", topic, invalidConfigMsgs));
        }
    }
}
