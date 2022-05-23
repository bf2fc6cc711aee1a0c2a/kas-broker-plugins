package io.bf2.kafka.common;

import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Set;

public class ConfigRules {
    public static final Set<ConfigRule> CONFIG_RULES = Set.of(
            new DefaultConfigValueRule(),
            new MaxMessageBytesRule(),
            new MinInsyncReplicasRule(),
            new NotAllowUpdateRule());

    public static boolean isValid(Map<String, String> configs) {
        for (ConfigRule rule : CONFIG_RULES) {
            if (!rule.IsValid(configs)) {
                return false;
            }
        }
        return true;
    }
}
