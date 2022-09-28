package io.bf2.kafka.common.rule;

import scala.collection.immutable.Set$;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a rule that doesn't allow the config value to be updated
 */
public class ImmutableRule implements ConfigRule {
    private final Set<String> mutableConfigs;
    private final Map<String, String> defaults;

    public ImmutableRule(Set<String> mutableConfigs) {
        this.mutableConfigs = mutableConfigs;

        this.defaults = new kafka.log.LogConfig(Collections.emptyMap(), Set$.MODULE$.empty())
            .values()
            .entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().toString()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Optional<String> validate(String key, String val) {
        String defaultVal = defaults.get(key);

        if (!mutableConfigs.contains(key) && !Objects.equals(defaultVal, val)) {
            return Optional.of(String.format(
                    "Topic configured with invalid configs: %s=%s. This config cannot be updated.", key, val));
        }
        return Optional.empty();
    }
}
