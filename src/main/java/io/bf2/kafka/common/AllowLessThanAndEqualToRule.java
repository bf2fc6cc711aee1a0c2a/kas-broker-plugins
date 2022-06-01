package io.bf2.kafka.common;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.MAX_MESSAGE_BYTES_CONFIG;

/**
 * This is a rule that only allow configs using default value
 */
public class AllowLessThanAndEqualToRule implements ConfigRule {
    private final Map<String, String> lessThanAndEqualToConfigs;

    public AllowLessThanAndEqualToRule(Map<String, String> lessThanAndEqualToConfigs) {
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
