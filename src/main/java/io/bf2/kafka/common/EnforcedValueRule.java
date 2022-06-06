package io.bf2.kafka.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a rule that only allows configs using the provided value
 */
public class EnforcedValueRule implements ConfigRule {
    private final Map<String, String> configWithDefaultValue;

    public EnforcedValueRule(Map<String, String> configWithDefaultValue) {
        this.configWithDefaultValue = configWithDefaultValue;
    }

    @Override
    public List<InvalidConfig> validate(String topic, Map<String, String> configs) {
        List<InvalidConfig> invalidConfigs = new ArrayList<>();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (configWithDefaultValue.containsKey(key)) {
                String defaultVal = configWithDefaultValue.get(key);
                String val = entry.getValue();
                if (!val.equals(defaultVal)) {
                    invalidConfigs.add(new InvalidConfig(key, val, defaultVal, InvalidConfig.RuleType.ENFORCED_VALUE_RULE));
                }
            }
        }
        return invalidConfigs;
    }
}
