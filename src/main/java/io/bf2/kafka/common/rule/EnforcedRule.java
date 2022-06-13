package io.bf2.kafka.common.rule;

import java.util.Map;
import java.util.Optional;

/**
 * This is a rule that only allows configs using the expected value
 */
public class EnforcedRule implements ConfigRule {
    private final Map<String, String> enforcedConfigs;

    public EnforcedRule(Map<String, String> configWithDefaultValue) {
        this.enforcedConfigs = configWithDefaultValue;
    }

    @Override
    public Optional<String> validate(String key, String val) {
        if (enforcedConfigs.containsKey(key)) {
            String expectedValue = enforcedConfigs.get(key);
            if (!val.equals(expectedValue)) {
                return Optional.of(String.format(
                        "Topic configured with invalid configs: %s=%s. Please don't try to set this property, or only try to set it to: %s.", key, val, expectedValue));
            }
        }
        return Optional.empty();
    }
}
