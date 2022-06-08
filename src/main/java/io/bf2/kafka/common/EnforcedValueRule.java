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
    public Optional<InvalidConfig> validate(String key, String val) {
        if (configWithDefaultValue.containsKey(key)) {
            String defaultVal = configWithDefaultValue.get(key);
            if (!val.equals(defaultVal)) {
                return Optional.of(new InvalidConfig(key, val, defaultVal, InvalidConfig.RuleType.ENFORCED_VALUE_RULE));
            }
        }
        return Optional.empty();
    }
}
