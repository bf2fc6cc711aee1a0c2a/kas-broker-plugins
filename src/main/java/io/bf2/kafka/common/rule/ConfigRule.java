package io.bf2.kafka.common.rule;

import java.util.Optional;

/**
 * To validate if the config is valid for a specific rule
 */
public interface ConfigRule {

    /**
     * Validate if the config is valid for a specific rule, and return the error message if any
     *
     * @param key the config key to be customized
     * @param value the config value to be customized
     * @return optional error message if the provided configs violate the rule, otherwise, an empty optional instance will be returned
     */
    Optional<String> validate(String key, String value);
}
