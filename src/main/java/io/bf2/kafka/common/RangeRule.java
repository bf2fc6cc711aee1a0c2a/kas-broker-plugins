package io.bf2.kafka.common;

import com.google.common.collect.Range;

import java.util.Map;
import java.util.Optional;

/**
 * This is a rule that allows values within a provided range
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
            double doubleVal;
            try {
                doubleVal = Double.parseDouble(val);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("The provided value is not a number: %s=%s", key, val), e);
            }
            Range range = configRange.get(key);

            if (!range.contains(doubleVal)) {
                return Optional.of(new InvalidConfig(key, val, range, InvalidConfig.RuleType.RANGE_RULE));
            }
        }
        return Optional.empty();
    }
}
