package io.bf2.kafka.common;

import com.google.common.collect.Range;

/**
 * A class contains the invalid config and the violated rule info
 */
public class InvalidConfig {
    private final  String config;
    private final String value;
    private final String expectedValue;
    private final RuleType ruleType;
    private final Range range;

    public enum RuleType {
        RANGE_RULE,
        ENFORCED_VALUE_RULE,
        IMMUTABLE_RULE
    }

    public InvalidConfig(String config, String value, Range range, RuleType ruleType) {
        this(config, value, null, ruleType, range);
    }

    public InvalidConfig(String config, String value, RuleType ruleType) {
       this(config, value, null, ruleType, null);
    }

    public InvalidConfig(String config, String value, String expectedValue, RuleType ruleType) {
        this(config, value, expectedValue, ruleType, null);
    }

    public InvalidConfig(String config, String value, String expectedValue, RuleType ruleType, Range range) {
        this.config = config;
        this.value = value;
        this.expectedValue = expectedValue;
        this.ruleType = ruleType;
        this.range = range;
    }

    @Override
    public String toString() {
        switch (ruleType) {
            case ENFORCED_VALUE_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. Please don't try to set this property, or only try to set it to: %s.", config, value, expectedValue);
            case IMMUTABLE_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. This config cannot be updated.", config, value);
            case RANGE_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. The value of this property should be in this range: %s.", config, value, range);
            default:
                return String.format("ValidateResult(config = %d, value = %s, expected value = %s, range = %s, rule type = %s)",
                        config,
                        value,
                        expectedValue,
                        range,
                        ruleType);
        }
    }
}
