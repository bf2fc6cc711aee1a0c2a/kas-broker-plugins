package io.bf2.kafka.common;

/**
 * A class contain the invalid configs and the violated rule info
 */
public class InvalidConfig {
    final private String config;
    final private String value;
    final private String expectedValue;
    final private RuleType ruleType;

    public enum RuleType {
        ALLOW_LESS_THAN_OR_EQUAL_RULE,
        ENFORCED_VALUE_RULE,
        NOT_ALLOW_UPDATE_RULE
    }

    public InvalidConfig(String config, String value, RuleType ruleType) {
        this.config = config;
        this.value = value;
        this.ruleType = ruleType;
        this.expectedValue = null;
    }

    public InvalidConfig(String config, String value, String expectedValue, RuleType ruleType) {
        this.config = config;
        this.value = value;
        this.expectedValue = expectedValue;
        this.ruleType = ruleType;
    }

    @Override
    public String toString() {
        switch (ruleType) {
            case ENFORCED_VALUE_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. Please don't try to set this property, or only try to set it to: %s.", config, value, expectedValue);
            case NOT_ALLOW_UPDATE_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. This config cannot be updated.", config, value);
            case ALLOW_LESS_THAN_OR_EQUAL_RULE:
                return String.format("Topic configured with invalid configs: %s=%s. The value of this property cannot be above %s.", config, value, expectedValue);
            default:
                return String.format("ValidateResult(config = %d, value = %s, expected value = %s, rule type = %s)",
                        config,
                        value,
                        expectedValue,
                        ruleType);
        }
    }
}
