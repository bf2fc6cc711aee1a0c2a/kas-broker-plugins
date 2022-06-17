package io.bf2.kafka.common.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.bf2.kafka.common.Config;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

        enforcedConfigs = parseListToMap(parsedConfig.getList(Config.ENFORCED_VALUE_CONFIGS));
        rangeConfigs = parseListToRangeMap(parsedConfig.getList(Config.RANGE_CONFIGS));

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


    /**
     * This method gets comma separated values which contains key,value pairs and returns a map of
     * key value pairs. the format of string is key1:val1,key2:val2 ....
     *
     * @param list the list with the format: key1:val1,key2:val2
     * @return  the unmodifiable map with the {key1=val1, key2=val2}
     */
    public Map<String, String> parseListToMap(List<String> list) {
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
    public Map<String, Range<Double>> parseListToRangeMap(List<String> list) {
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
