package io.bf2.kafka.common;

import com.google.common.collect.Range;
import io.bf2.kafka.common.rule.ConfigRules;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigRulesTest {
    private static String DUMMY_CONFIG_KEY = "dummyConfig";
    private ConfigRules defaultConfigRules = new ConfigRules(configWith(Collections.emptyMap()));

    @Test
    void testGetDefaultRuleConfigsWithPolicyDisabled() {
        assertEquals(Collections.emptyMap(), defaultConfigRules.getEnforcedConfigs());
        assertEquals(Collections.emptySet(), defaultConfigRules.getMutableConfigs());
        assertEquals(Collections.emptyMap(), defaultConfigRules.getRangeConfigs());
        assertFalse(defaultConfigRules.getIsTopicConfigPolicyEnabled());
    }

    @Test
    void testGetDefaultRuleConfigsWithPolicyEnabled() {
        Map<String, ?> config = configWith(Map.of(Config.TOPIC_CONFIG_POLICY_ENFORCED, true));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(defaultConfigRules.parseListToMap(DUMMY_CONFIG_KEY, List.copyOf(Config.DEFAULT_ENFORCED_VALUE_SET)), configRules.getEnforcedConfigs());
        assertEquals(Config.DEFAULT_MUTABLE_CONFIG_KEYS, configRules.getMutableConfigs());
        assertEquals(defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, List.copyOf(Config.DEFAULT_RANGE_CONFIG_SET)), configRules.getRangeConfigs());
        assertTrue(configRules.getIsTopicConfigPolicyEnabled());
    }

    @Test
    void testGetCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                Config.ENFORCED_VALUE_CONFIGS, "min.compaction.lag.ms:0",
                Config.RANGE_CONFIGS, "retention.ms::604800000,min.cleanable.dirty.ratio:0.5:,segment.index.bytes:1000:10000",
                Config.MUTABLE_CONFIGS, "compression.type",
                Config.TOPIC_CONFIG_POLICY_ENFORCED, true));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("min.compaction.lag.ms", "0"), configRules.getEnforcedConfigs());
        Set<String> expectedMutableConfigs = Set.of("compression.type", "min.compaction.lag.ms", "segment.index.bytes", "retention.ms", "min.cleanable.dirty.ratio");
        assertEquals(expectedMutableConfigs, configRules.getMutableConfigs());

        Map<String, Range<Double>> expectedRangeConfigs = new HashMap<>(Map.of(
                "min.cleanable.dirty.ratio", Range.atLeast((double)0.5),
                "retention.ms", Range.atMost((double)604800000),
                "segment.index.bytes", Range.closed((double)1000, (double)10000)));
        assertEquals(expectedRangeConfigs, configRules.getRangeConfigs());
    }

    @Test
    void testMutableConfigsShouldContainAllConfigs() {
        Map<String, ?> config = configWith(Map.of(
                Config.ENFORCED_VALUE_CONFIGS, "min.compaction.lag.ms:0",
                Config.RANGE_CONFIGS, "retention.ms::604800000",
                Config.MUTABLE_CONFIGS, "compression.type",
                Config.TOPIC_CONFIG_POLICY_ENFORCED, true));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Set.of("compression.type", "min.compaction.lag.ms", "retention.ms"), configRules.getMutableConfigs());
    }

    @Test
    void testGetDuplicatedCustomRuleConfigs() {
        Map<String, ?> config = configWith(Map.of(
                Config.ENFORCED_VALUE_CONFIGS, "retention.ms:604800000",
                Config.MUTABLE_CONFIGS, "retention.ms",
                Config.RANGE_CONFIGS, "",
                Config.TOPIC_CONFIG_POLICY_ENFORCED, true));

        ConfigRules configRules = new ConfigRules(config);
        assertEquals(Map.of("retention.ms", "604800000"), configRules.getEnforcedConfigs());
        assertEquals(Set.of("retention.ms"), configRules.getMutableConfigs());
    }

    @Test
    void parseListToMapShouldReturnEmptyMapWithNullList() {
        assertEquals(Collections.emptyMap(), defaultConfigRules.parseListToMap(DUMMY_CONFIG_KEY, null));
    }

    @Test
    void parseListToMapShouldReturnEmptyMapWithEmptyList() {
        assertEquals(Collections.emptyMap(), defaultConfigRules.parseListToMap(DUMMY_CONFIG_KEY, Collections.emptyList()));
    }

    @Test
    void parseListToMapShouldReturnExpectedMap() {
        List<String> configList = List.of(
                "x.y.z:123",
                "xx.yy.zz:0.5",
                "xxx.yyy.zzz:abc"
        );
        Map<String, String> expectedMap = Map.of(
                "x.y.z", "123",
                "xx.yy.zz", "0.5",
                "xxx.yyy.zzz", "abc"
        );
        assertEquals(expectedMap, defaultConfigRules.parseListToMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToMapShouldThrowExceptionIfBadFormat() {
        List<String> configList = List.of(
                "x.y.z:123",
                "xx.yy.zz:0.5",
                "bad.format"
        );
        assertThrows(IllegalArgumentException.class, () -> defaultConfigRules.parseListToMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToRangeMapShouldReturnEmptyMapWithNullList() {
        assertEquals(Collections.emptyMap(), defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, Collections.emptyList()));
    }

    @Test
    void parseListToRangeMapShouldReturnEmptyMapWithEmptyList() {
        assertEquals(Collections.emptyMap(), defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, Collections.emptyList()));
    }

    @Test
    void parseListToRangeMapShouldReturnExpectedMap() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "xxx.yyy.zzz::500"
        );
        Map<String, Range<Double>> expectedMap = Map.of(
                "x.y.z", Range.closed((double)100, (double)200),
                "xx.yy.zz", Range.atLeast((double)0.5),
                "xxx.yyy.zzz", Range.atMost((double)500)
        );
        assertEquals(expectedMap, defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfBadFormat() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:123"
        );
        assertThrows(IllegalArgumentException.class, () -> defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfMinIsNotNumber() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:abc:123"
        );
        assertThrows(IllegalArgumentException.class, () -> defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfMaxIsNotNumber() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format:123:abc"
        );
        assertThrows(IllegalArgumentException.class, () -> defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, configList));
    }

    @Test
    void parseListToRangeMapShouldThrowExceptionIfNoMinAndMax() {
        List<String> configList = List.of(
                "x.y.z:100:200",
                "xx.yy.zz:0.5:",
                "bad.format::"
        );
        assertThrows(IllegalArgumentException.class, () -> defaultConfigRules.parseListToRangeMap(DUMMY_CONFIG_KEY, configList));
    }

    private Map<String, ?> configWith(Map<String, ?> customEntries) {
        Map<String, ?> defaults = Map.of(
                Config.MAX_PARTITIONS, 1000,
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");

        Map<String, Object> result = new HashMap<>(customEntries);
        defaults.entrySet().forEach(e -> result.putIfAbsent(e.getKey(), e.getValue()));
        return result;
    }
}
