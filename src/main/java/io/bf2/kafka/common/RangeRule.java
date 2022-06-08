package io.bf2.kafka.common;

import com.google.common.collect.Range;

import java.util.Map;
import java.util.Optional;

/**
 * This is a rule that allows values with less than or equal to a specific value
 */
public class RangeRule implements ConfigRule {
    private final Map<String, Range> configRange;

    public RangeRule(Map<String, Range> configRange) {
        this.configRange = configRange;
    }

    @Override
    public Optional<InvalidConfig> validate(String key, String val) {
        if (configRange.containsKey(key)) {
            // convert the number into double for accurate comparison
            double doubleVal = Double.parseDouble(val);
            Range range = configRange.get(key);
            System.out.println("!!! key:" + key + ",val:" + val);
            System.out.println("!!! range:" + range);

            if (!range.contains(doubleVal)) {
                return Optional.of(new InvalidConfig(key, val, range, InvalidConfig.RuleType.RANGE_RULE));
            }
        }
        return Optional.empty();
    }
}
