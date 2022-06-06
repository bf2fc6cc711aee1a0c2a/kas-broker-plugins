package io.bf2.kafka.common;

import java.util.List;
import java.util.Map;

/**
 * To validate if the configs are valid for a specific rule
 */
public interface ConfigRule {

    /**
     * Validate if the configs are valid for a specific rule, and return all the invalid configs
     *
     * @param topic the topic to be created or alter configs
     * @param configs the configs to be customized
     * @return list of {@link InvalidConfig} if the provided configs violate the rule, otherwise, an empty list will be returned
     */
    List<InvalidConfig> validate(String topic, Map<String, String> configs);
}
