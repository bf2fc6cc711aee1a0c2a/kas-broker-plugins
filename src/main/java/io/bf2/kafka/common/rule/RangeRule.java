package io.bf2.kafka.common.rule;

import com.google.common.collect.Range;

import java.util.Map;
import java.util.Optional;

/**
 * This is a rule that allows values within a provided range
 */
public class RangeRule implements ConfigRule {
    private final Map<String, Range<Double>> configRange;

    public RangeRule(Map<String, Range<Double>> configRange) {
        this.configRange = configRange;
    }

    @Override
    public Optional<String> validate(String key, String val) {
        if (configRange.containsKey(key)) {
            // convert the number into double for accurate comparison
            double doubleVal;
            try {
                doubleVal = Double.parseDouble(val);
            } catch (NumberFormatException e) {
                return Optional.of(String.format("The provided value is not a number: %s=%s. The error is: %s.", key, val, e.getMessage()));
            }
            Range<Double> range = configRange.get(key);

            if (!range.contains(doubleVal)) {
                return Optional.of(String.format(
                        "Topic configured with invalid configs: %s=%s. The value of this property should be in this range: %s.", key, val, range));
            }
        }
        return Optional.empty();
    }
}
