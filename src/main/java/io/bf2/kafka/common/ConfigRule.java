package io.bf2.kafka.common;

import java.util.Map;

public interface ConfigRule {
    /**
     * To validate if the configs is valid for a specific rule
     */
    void validate(String topic, Map<String, String> configs);
}
