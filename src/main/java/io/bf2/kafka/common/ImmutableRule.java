package io.bf2.kafka.common;

import java.util.Optional;
import java.util.Set;

/**
 * This is a rule that doesn't allow the config value to be updated
 */
public class ImmutableRule implements ConfigRule {
    private final Set<String> mutableConfigs;

    public ImmutableRule(Set<String> mutableConfigs) {
        this.mutableConfigs = mutableConfigs;
    }

    @Override
    public Optional<String> validate(String key, String val) {
        if (!mutableConfigs.contains(key)) {
            return Optional.of(String.format(
                    "Topic configured with invalid configs: %s=%s. This config cannot be updated.", key, val));
        }
        return Optional.empty();
    }
}
