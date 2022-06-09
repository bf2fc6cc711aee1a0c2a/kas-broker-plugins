package io.bf2.kafka.common;

import java.util.Map;
import java.util.Optional;

/**
 * This is a rule that only allows configs using the provided value
 */
public class EnforcedValueRule implements ConfigRule {
    private final Map<String, String> configWithDefaultValue;

    public EnforcedValueRule(Map<String, String> configWithDefaultValue) {
        this.configWithDefaultValue = configWithDefaultValue;
    }

    @Override
    public Optional<String> validate(String key, String val) {
        if (configWithDefaultValue.containsKey(key)) {
            String expectedValue = configWithDefaultValue.get(key);
            if (!val.equals(expectedValue)) {
                return Optional.of(String.format(
                        "Topic configured with invalid configs: %s=%s. Please don't try to set this property, or only try to set it to: %s.", key, val, expectedValue));
            }
        }
        return Optional.empty();
    }
}
