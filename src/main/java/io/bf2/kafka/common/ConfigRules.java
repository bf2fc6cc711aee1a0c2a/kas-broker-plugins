package io.bf2.kafka.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.*;
import static org.apache.kafka.common.config.TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG;

public class ConfigRules {
    // should be updated after https://github.com/bf2fc6cc711aee1a0c2a/kafka-group-authorizer/pull/37 merged
    public static final String TOPIC_CONFIG_POLICY_PREFIX = "kas.policy." + "topic-config.";

    public static final String ALLOW_DEFAULT_CONFIG_VALUE_CONFIGS = TOPIC_CONFIG_POLICY_PREFIX + "default";
    public static final String NOT_ALLOW_UPDATE_CONFIGS = TOPIC_CONFIG_POLICY_PREFIX + "no.update";
    public static final String LESS_THAN_AND_EQUAL_TO_CONFIGS = TOPIC_CONFIG_POLICY_PREFIX + "less.equal";

    public static final Set<String> DEFAULT_CONFIG_WITH_DEFAULT_VALUE_SET = Set.of(
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
    public static final Set<String> DEFAULT_LESS_THAN_AND_EQUAL_TO_CONFIG_SET = Set.of(MAX_MESSAGE_BYTES_CONFIG + ":" + DEFAULT_MAX_MESSAGE_BYTE_ALLOWED);

    public static final String DEFAULT_CONFIG_VALUE_CONFIGS = String.join(",", DEFAULT_CONFIG_WITH_DEFAULT_VALUE_SET);
    public static final String DEFAULT_NOT_ALLOW_UPDATE_CONFIGS = String.join(",", DEFAULT_CONFIG_CANNOT_UPDATE_SET);
    public static final String DEFAULT_LESS_THAN_AND_EQUAL_TO_CONFIGS = String.join(",", DEFAULT_LESS_THAN_AND_EQUAL_TO_CONFIG_SET);

    public final Set<ConfigRule> CONFIG_RULES = new HashSet<>();

    public static final ConfigDef configDef = new ConfigDef()
            .define(ALLOW_DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Type.STRING, DEFAULT_CONFIG_VALUE_CONFIGS, ConfigDef.Importance.MEDIUM, "Feature flag to allow enabling of partition limit enforcement")
            .define(NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Type.STRING, DEFAULT_NOT_ALLOW_UPDATE_CONFIGS, ConfigDef.Importance.MEDIUM, "Max partitions")
            .define(LESS_THAN_AND_EQUAL_TO_CONFIGS, ConfigDef.Type.STRING, DEFAULT_LESS_THAN_AND_EQUAL_TO_CONFIGS, ConfigDef.Importance.MEDIUM, "Internal Partition Prefix");

    private Map<String, String> defaultValueConfigs;
    private Set<String> notAllowUpdateConfigs;
    private Map<String, String> lessThanAndEqualToConfigs;

    public ConfigRules(Map<String, ?> configs) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, configs);

        defaultValueConfigs = parseCsvMap(parsedConfig.getString(ALLOW_DEFAULT_CONFIG_VALUE_CONFIGS));
        notAllowUpdateConfigs = parseCsvSet(parsedConfig.getString(NOT_ALLOW_UPDATE_CONFIGS));
        lessThanAndEqualToConfigs = parseCsvMap(parsedConfig.getString(LESS_THAN_AND_EQUAL_TO_CONFIGS));
        CONFIG_RULES.add(new DefaultConfigValueRule(defaultValueConfigs));
        CONFIG_RULES.add(new NotAllowUpdateRule(notAllowUpdateConfigs));
        CONFIG_RULES.add(new AllowLessThanAndEqualToRule(lessThanAndEqualToConfigs));
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
     * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
     * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
     * of the ":" in the pair as the split, eg a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
     */
    public Map<String, String> parseCsvMap(String str) {
        Map<String, String> map = new HashMap<>(str.length());

        Arrays.stream(str.split("\\s*,\\s*")).forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            map.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });
        return map;
    }



    /**
     * Parse a comma separated string into a sequence of strings.
     * Whitespace surrounding the comma will be removed.
     */
    public Set<String> parseCsvSet(String csvList) {
        if (csvList == null || csvList.isEmpty())
            return Collections.EMPTY_SET;
        else
            return Arrays.stream(csvList.split("\\s*,\\s*")).filter(v -> !v.equals("")).collect(Collectors.toSet());
    }
}
