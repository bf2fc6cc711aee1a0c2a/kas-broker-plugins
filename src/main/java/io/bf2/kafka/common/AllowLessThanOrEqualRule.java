package io.bf2.kafka.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a rule that allows values with less than or equal to a specific value
 */
public class AllowLessThanOrEqualRule implements ConfigRule {
    private final Map<String, String> lessThanOrEqualConfigs;

    public AllowLessThanOrEqualRule(Map<String, String> lessThanOrEqualConfigs) {
        this.lessThanOrEqualConfigs = lessThanOrEqualConfigs;
    }

    @Override
    public List<InvalidConfig> validate(String topic, Map<String, String> configs) {
        List<InvalidConfig> invalidConfigs = new ArrayList<>();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (lessThanOrEqualConfigs.containsKey(key)) {
                String val = entry.getValue();
                String upperBound = lessThanOrEqualConfigs.get(key);

                // convert the number into double for comparison
                double doubleVal = Double.parseDouble(val);
                double doubleUpperBound = Double.parseDouble(upperBound);

                if (doubleVal > doubleUpperBound) {
                    invalidConfigs.add(new InvalidConfig(key, val, upperBound, InvalidConfig.RuleType.ALLOW_LESS_THAN_OR_EQUAL_RULE));
                }
            }
        }
        return invalidConfigs;
    }
}
