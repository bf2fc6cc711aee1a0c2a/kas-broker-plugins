package io.bf2.kafka.common;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.*;

/**
 * This is a rule that only allow configs using default value
 */
public class NotAllowUpdateRule implements ConfigRule {
    private final Set<String> notAllowUpdateConfigs;

    public NotAllowUpdateRule(Set<String> notAllowUpdateConfigs) {
        this.notAllowUpdateConfigs = notAllowUpdateConfigs;
    }


    @Override
    public void validate(String topic, Map<String, String> configs) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (notAllowUpdateConfigs.contains(key)) {
                throw new PolicyViolationException(
                        String.format("Topic %s configured with invalid configs: %s:%s. This config cannot be updated."
                                , topic, key, entry.getValue()));
            }
        }
    }
}
