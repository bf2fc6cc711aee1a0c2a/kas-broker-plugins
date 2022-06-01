package io.bf2.kafka.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigRulesTest {

    @Test
    void testGetDefaultRuleConfigs() {
        Map<String, ?> config = configWith(Map.of());
        ConfigRules configRules = new ConfigRules(config);
        assertEquals(setToMap(ConfigRules.DEFAULT_CONFIG_WITH_ONE_VALUE_SET), configRules.getDefaultValueConfigs());
        assertEquals(ConfigRules.DEFAULT_CONFIG_CANNOT_UPDATE_SET, configRules.getNotAllowUpdateConfigs());
        assertEquals(setToMap(ConfigRules.DEFAULT_LESS_THAN_OR_EQUAL_TO_CONFIG_SET), configRules.getLessThanAndEqualToConfigs());
    }

    @Test
    void testGetCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                ConfigRules.ALLOW_ONE_CONFIG_VALUE_CONFIGS, "min.compaction.lag.ms:0",
                ConfigRules.LESS_THAN_OR_EQUAL_TO_CONFIGS, "retention.ms:604800000",
                ConfigRules.NOT_ALLOW_UPDATE_CONFIGS, "compression.type"));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("min.compaction.lag.ms", "0"), configRules.getDefaultValueConfigs());
        assertEquals(Set.of("compression.type"), configRules.getNotAllowUpdateConfigs());
        assertEquals(Map.of("retention.ms", "604800000"), configRules.getLessThanAndEqualToConfigs());
    }

    @Test
    void testGetDuplicatedCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                ConfigRules.ALLOW_ONE_CONFIG_VALUE_CONFIGS, "retention.ms:604800000",
                ConfigRules.LESS_THAN_OR_EQUAL_TO_CONFIGS, "retention.ms:604800000"));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("retention.ms", "604800000"), configRules.getDefaultValueConfigs());
        assertEquals(Map.of("retention.ms", "604800000"), configRules.getLessThanAndEqualToConfigs());
    }

    private Map<String, ?> configWith(Map<String, ?> customEntries) {
        Map<String, ?> defaults = Map.of(
                PartitionCounter.MAX_PARTITIONS, 1000,
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");

        Map<String, Object> result = new HashMap<>(customEntries);
        defaults.entrySet().forEach(e -> result.putIfAbsent(e.getKey(), e.getValue()));
        return result;
    }

    private Map<String, String> setToMap(Set<String> set) {
        Map<String, String> result = new HashMap<>();
        set.forEach(s -> {
            int delimiter = s.lastIndexOf(":");
            result.put(s.substring(0, delimiter).trim(), s.substring(delimiter + 1).trim());
        });

        return result;
    }


}
