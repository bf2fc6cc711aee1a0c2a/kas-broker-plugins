package io.bf2.kafka.common.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.bf2.kafka.common.Config;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A class that parses the user provided rule configs to create all rules, and validates the provided configs.
 */
public class ConfigRules {
    private final Set<ConfigRule> configRules;
    private final Map<String, String> enforcedConfigs;
    private final Set<String> mutableConfigs;
    private final Map<String, Range<Double>> rangeConfigs;

    public ConfigRules(Map<String, ?> configs) {
        AbstractConfig parsedConfig = new AbstractConfig(Config.TOPIC_POLICY_CONFIG_DEF, configs);

        enforcedConfigs = Config.parseListToMap(parsedConfig.getList(Config.ENFORCED_VALUE_CONFIGS));
        rangeConfigs = Config.parseListToRangeMap(parsedConfig.getList(Config.RANGE_CONFIGS));

        // mutable configs should be the union of all config keys
        mutableConfigs = ImmutableSet.<String>builder()
                .addAll(Set.copyOf(parsedConfig.getList(Config.MUTABLE_CONFIGS)))
                .addAll(enforcedConfigs.keySet())
                .addAll(rangeConfigs.keySet())
                .build();

        configRules = Set.of(
                new EnforcedRule(enforcedConfigs),
                new ImmutableRule(mutableConfigs),
                new RangeRule(rangeConfigs));
    }

    public Map<String, String> getEnforcedConfigs() {
        return enforcedConfigs;
    }

    public Map<String, Range<Double>> getRangeConfigs() {
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
