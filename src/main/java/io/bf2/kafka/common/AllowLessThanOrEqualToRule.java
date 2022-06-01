package io.bf2.kafka.common;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;

/**
 * This is a rule that allows values with less than or equal to a specific value
 */
public class AllowLessThanOrEqualToRule implements ConfigRule {
    private final Map<String, String> lessThanAndEqualToConfigs;

    public AllowLessThanOrEqualToRule(Map<String, String> lessThanAndEqualToConfigs) {
        this.lessThanAndEqualToConfigs = lessThanAndEqualToConfigs;
    }

    @Override
    public void validate(String topic, Map<String, String> configs) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (lessThanAndEqualToConfigs.containsKey(key)) {
                long val = Long.parseLong(entry.getValue());
                long upperBound = Long.parseLong(lessThanAndEqualToConfigs.get(key));
                if (val > upperBound) {
                    throw new PolicyViolationException(
                            String.format("Topic %s configured with invalid configs: %s:%s. This config only allow value lower or equal to %d"
                                    , topic, key, val, upperBound));
                }
            }
        }
    }
}
