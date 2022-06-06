package io.bf2.kafka.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.*;
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
    public static final String ENFORCED_VALUE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "one.value";

    /**
     * Custom broker property key, used to specify the configs that cannot be updated. It's a comma separated list.
     * If this property is not specified, a default of {@link #DEFAULT_CONFIG_CANNOT_UPDATE_SET} will be used in this class.
     */
    public static final String NOT_ALLOW_UPDATE_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "no.update";

    /**
     * Custom broker property key, used to specify the configs that allow values less than or equal to a provided value. 
     * If this property is not specified, a default of {@link #DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET} will be used in this class.
     */
    public static final String LESS_THAN_OR_EQUAL_TO_CONFIGS = ALTER_CONFIG_POLICY_PREFIX + "less.equal";

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

    public static final String DEFAULT_CONFIG_VALUE_CONFIGS = String.join(",", DEFAULT_ENFORCED_VALUE_SET);
    public static final String DEFAULT_NOT_ALLOW_UPDATE_CONFIGS = String.join(",", DEFAULT_CONFIG_CANNOT_UPDATE_SET);
    public static final String DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIGS = String.join(",", DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET);

    public final Set<ConfigRule> configRules;

    public static final ConfigDef configDef = new ConfigDef()
            .define(ENFORCED_VALUE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Importance.MEDIUM, "This is used to specify the configs with permitted value. It's a list in the form [keyA]:[valueA],[keyB]:[valueB]")
            .define(NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Type.LIST, DEFAULT_NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Importance.MEDIUM, "This is used to specify the configs that cannot be updated. It's a comma separated list.")
            .define(LESS_THAN_OR_EQUAL_TO_CONFIGS, ConfigDef.Type.LIST, DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIGS, ConfigDef.Importance.MEDIUM, "This is used to specify the configs that allow values less than or equal to a provided value.");

    private Map<String, String> defaultValueConfigs;
    private Set<String> notAllowUpdateConfigs;
    private Map<String, String> lessThanOrEqualConfigs;

    public ConfigRules(Map<String, ?> configs) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, configs);

        defaultValueConfigs = parseListToMap(parsedConfig.getList(ENFORCED_VALUE_CONFIGS));
        notAllowUpdateConfigs = Set.copyOf(parsedConfig.getList(NOT_ALLOW_UPDATE_CONFIGS));
        lessThanOrEqualConfigs = parseListToMap(parsedConfig.getList(LESS_THAN_OR_EQUAL_TO_CONFIGS));
        configRules = Set.of(
                new EnforcedValueRule(defaultValueConfigs),
                new NotAllowUpdateRule(notAllowUpdateConfigs),
                new AllowLessThanOrEqualRule(lessThanOrEqualConfigs));
    }

    public Map<String, String> getDefaultValueConfigs() {
        return defaultValueConfigs;
    }

    public Map<String, String> getLessThanOrEqualConfigs() {
        return lessThanOrEqualConfigs;
    }

    public Set<String> getNotAllowUpdateConfigs() {
        return notAllowUpdateConfigs;
    }

    public void validateTopicConfigs(String topic, Map<String, String> configs) {
        List<InvalidConfig> invalidConfigs = new ArrayList<>();
        for (ConfigRule rule : configRules) {
            invalidConfigs.addAll(rule.validate(topic, configs));
        }

        if (!invalidConfigs.isEmpty()) {
            throw new PolicyViolationException(
                    String.format("Invalid config specified for topic %s. The violated configs are: %s", topic, invalidConfigs));
        }
    }

    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of string is key1:val1,key2:val2 ....
     *
     * @param list the list with the format: key1:val1,key2:val2
     * @return  the unmodifiable map with the {key1=val1, key2=val2}
     */
    private Map<String, String> parseListToMap(List<String> list) {
        if (list == null || list.isEmpty())
            return Collections.emptyMap();

        Map<String, String> map = new HashMap<>(list.size());

        list.stream().forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            map.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });
        return Map.copyOf(map);
    }
}
