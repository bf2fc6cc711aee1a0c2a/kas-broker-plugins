package io.bf2.kafka.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.*;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

/**
 * A class that parse the user provided rule configs to create all rules, and validates the provided configs.  
 */
public class ConfigRules {
    public static final String ALTER_CONFIG_POLICY_PREFIX = Config.POLICY_PREFIX + "topic-config.";

    /**
     * Custom broker property key, used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB].
     * If this property is not specified, a default of {@link #DEFAULT_CONFIG_WITH_ONE_VALUE_SET} will be used in this class.
     */
    public static final String ALLOW_ONE_CONFIG_VALUE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "one.value";

    /**
     * Custom broker property key, used to specify the configs that cannot be updated. It's a comma seperated list.
     * If this property is not specified, a default of {@link #DEFAULT_CONFIG_CANNOT_UPDATE_SET} will be used in this class.
     */
    public static final String NOT_ALLOW_UPDATE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "no.update";

    /**
     * Custom broker property key, used to specify the configs that allow values less than or equal to a provided value. 
     * If this property is not specified, a default of {@link #DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET} will be used in this class.
     */
    public static final String LESS_THAN_OR_EQUAL_TO_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "less.equal";

    public static final Set<String> DEFAULT_CONFIG_WITH_ONE_VALUE_SET = Set.of(
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

    public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
    public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
    public static final Set<String> DEFAULT_CONFIG_CANNOT_UPDATE_SET = Set.of(
            MESSAGE_FORMAT_VERSION_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            MIN_IN_SYNC_REPLICAS_CONFIG
    );

    public static final String DEFAULT_MAX_MESSAGE_BYTE_ALLOWED = "1048588";
    public static final Set<String> DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET = Set.of(MAX_MESSAGE_BYTES_CONFIG + ":" + DEFAULT_MAX_MESSAGE_BYTE_ALLOWED);

    public static final String DEFAULT_CONFIG_VALUE_CONFIGS = String.join(",", DEFAULT_CONFIG_WITH_ONE_VALUE_SET);
    public static final String DEFAULT_NOT_ALLOW_UPDATE_CONFIGS = String.join(",", DEFAULT_CONFIG_CANNOT_UPDATE_SET);
    public static final String DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIGS = String.join(",", DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET);

    public final Set<ConfigRule> CONFIG_RULES = new HashSet<>();

    public static final ConfigDef configDef = new ConfigDef()
            .define(ALLOW_ONE_CONFIG_VALUE_CONFIGS, ConfigDef.Type.STRING, DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Importance.MEDIUM, "Feature flag to allow enabling of partition limit enforcement")
            .define(NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Type.STRING, DEFAULT_NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Importance.MEDIUM, "Max partitions")
            .define(LESS_THAN_OR_EQUAL_TO_CONFIGS, ConfigDef.Type.STRING, DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIGS, ConfigDef.Importance.MEDIUM, "Internal Partition Prefix");

    private Map<String, String> defaultValueConfigs;
    private Set<String> notAllowUpdateConfigs;
    private Map<String, String> lessThanAndEqualToConfigs;

    public ConfigRules(Map<String, ?> configs) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, configs);

        defaultValueConfigs = parseStrToMap(parsedConfig.getString(ALLOW_ONE_CONFIG_VALUE_CONFIGS));
        notAllowUpdateConfigs = parseStrToSet(parsedConfig.getString(NOT_ALLOW_UPDATE_CONFIGS));
        lessThanAndEqualToConfigs = parseStrToMap(parsedConfig.getString(LESS_THAN_OR_EQUAL_TO_CONFIGS));
        CONFIG_RULES.add(new OneValueAllowedConfigRule(defaultValueConfigs));
        CONFIG_RULES.add(new NotAllowUpdateRule(notAllowUpdateConfigs));
        CONFIG_RULES.add(new AllowLessThanOrEqualToRule(lessThanAndEqualToConfigs));
    }

    public Map<String, String> getDefaultValueConfigs() {
        return defaultValueConfigs;
    }

    public Map<String, String> getLessThanAndEqualToConfigs() {
        return lessThanAndEqualToConfigs;
    }

    public Set<String> getNotAllowUpdateConfigs() {
        return notAllowUpdateConfigs;
    }

    public void validateTopicConfigs(String topic, Map<String, String> configs) {
        for (ConfigRule rule : CONFIG_RULES) {
            rule.validate(topic, configs);
        }
    }

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of string is key1:val1,key2:val2 ....
     *
     * @param str the string with the format: key1:val1,key2:val2
     * @return  the map with the {key1=val1, key2=val2}
     */
    public Map<String, String> parseStrToMap(String str) {
        if (str == null || str.isEmpty())
            return Collections.EMPTY_MAP;

        Map<String, String> map = new HashMap<>(str.length());

        Arrays.stream(str.split("\\s*,\\s*")).forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            map.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });
        return map;
    }

    /**
     * Parse a comma separated string into a sequence of strings. the format of string is val1,val2,....
     * Whitespace surrounding the comma will be removed.
     *
     * @param str the string with the format: val1,val2
     * @return  the set with the {val1, val2}
     */
    public Set<String> parseStrToSet(String str) {
        if (str == null || str.isEmpty())
            return Collections.EMPTY_SET;
        else
            return Arrays.stream(str.split("\\s*,\\s*")).filter(v -> !v.equals("")).collect(Collectors.toSet());
    }
}
