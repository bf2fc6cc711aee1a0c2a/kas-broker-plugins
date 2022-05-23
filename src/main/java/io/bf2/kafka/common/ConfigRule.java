package io.bf2.kafka.common;

import java.util.Map;

public interface ConfigRule {
    /**
     * To validate if the configs is valid for a specific rule
     */
    boolean IsValid(Map<String, String> configs);
}
