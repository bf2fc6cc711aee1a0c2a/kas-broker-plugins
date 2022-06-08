package io.bf2.kafka.common;

import java.util.Optional;

/**
 * To validate if the config is valid for a specific rule
 */
public interface ConfigRule {

    /**
     * Validate if the config is valid for a specific rule, and return the invalid config if any
     *
     * @param key the config key to be customized
     * @param key the config value to be customized
     * @return {@link InvalidConfig} if the provided configs violate the rule, otherwise, an empty list will be returned
     */
    Optional<InvalidConfig> validate(String key, String value);
}
