package io.bf2.kafka.common;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;

/**
 * This is a rule that only allow configs using the provided value
 */
public class OneValueAllowedConfigRule implements ConfigRule {
    private final Map<String, String> configWithDefaultValue;

    public OneValueAllowedConfigRule(Map<String, String> configWithDefaultValue) {
        this.configWithDefaultValue = configWithDefaultValue;
    }

    @Override
    public void validate(String topic, Map<String, String> configs) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (configWithDefaultValue.containsKey(key)) {
                String defaultVal = configWithDefaultValue.get(key);
                String val = entry.getValue();
                if (!val.equals(defaultVal)) {
                    throw new PolicyViolationException(
                            String.format("Topic %s configured with invalid configs: %s:%s. This config only allow the value: %s"
                                    , topic, key, val, defaultVal));
                }
            }
        }
    }
}
