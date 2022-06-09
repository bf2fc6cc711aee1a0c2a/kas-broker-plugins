package io.bf2.kafka.common;

import com.google.common.collect.Range;
import io.bf2.kafka.common.rule.ConfigRules;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConfigRulesTest {

    @Test
    void testGetDefaultRuleConfigs() {
        Map<String, ?> config = configWith(Map.of());
        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Utils.parseListToMap(List.copyOf(ConfigRules.DEFAULT_ENFORCED_VALUE_SET)), configRules.getEnforcedConfigs());
        assertEquals(ConfigRules.DEFAULT_MUTABLE_CONFIG_KEYS, configRules.getMutableConfigs());
        assertEquals(Utils.parseListToRangeMap(List.copyOf(ConfigRules.DEFAULT_RANGE_CONFIG_SET)), configRules.getRangeConfigs());
    }

    @Test
    void testGetCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                ConfigRules.ENFORCED_VALUE_CONFIGS, "min.compaction.lag.ms:0",
                ConfigRules.RANGE_CONFIGS, "retention.ms::604800000,min.cleanable.dirty.ratio:0.5:,segment.index.bytes:1000:10000",
                ConfigRules.MUTABLE_CONFIGS, "compression.type"));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("min.compaction.lag.ms", "0"), configRules.getEnforcedConfigs());
        Set<String> expectedMutableConfigs = Set.of("compression.type", "min.compaction.lag.ms", "segment.index.bytes", "retention.ms", "min.cleanable.dirty.ratio");
        assertEquals(expectedMutableConfigs, configRules.getMutableConfigs());

        Map<String, Range> expectedRangeConfigs = new HashMap<>(Map.of(
                "min.cleanable.dirty.ratio", Range.atLeast((double)0.5),
                "retention.ms", Range.atMost((double)604800000),
                "segment.index.bytes", Range.closed((double)1000, (double)10000)));
        assertEquals(expectedRangeConfigs, configRules.getRangeConfigs());
    }

    @Test
    void testMutableConfigsShouldContainAllConfigs() {
        Map<String, ?> config = configWith(Map.of(
                ConfigRules.ENFORCED_VALUE_CONFIGS, "min.compaction.lag.ms:0",
                ConfigRules.RANGE_CONFIGS, "retention.ms::604800000",
                ConfigRules.MUTABLE_CONFIGS, "compression.type"));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Set.of("compression.type", "min.compaction.lag.ms", "retention.ms"), configRules.getMutableConfigs());
    }

    @Test
    void testGetDuplicatedCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                ConfigRules.ENFORCED_VALUE_CONFIGS, "retention.ms:604800000",
                ConfigRules.MUTABLE_CONFIGS, "retention.ms",
                ConfigRules.RANGE_CONFIGS, ""));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("retention.ms", "604800000"), configRules.getEnforcedConfigs());
        assertEquals(Set.of("retention.ms"), configRules.getMutableConfigs());
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
}
