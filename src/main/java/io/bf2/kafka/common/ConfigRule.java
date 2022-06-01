package io.bf2.kafka.common;

import java.util.Map;

/**
 * To validate if the configs is valid for a specific rule
 */
public interface ConfigRule {

    /**
     * Validate if the configs is valid for a specific rule
     *
     * @param topic the topic to be created or alter configs
     * @param configs the configs to be customized
     */
    void validate(String topic, Map<String, String> configs);
}
