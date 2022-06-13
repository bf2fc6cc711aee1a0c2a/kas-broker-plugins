package io.bf2.kafka.config;

import io.bf2.kafka.common.Config;
import io.bf2.kafka.common.LocalAdminClient;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


class ManagedKafkaAlterConfigPolicyTest {
    private static final String TOPIC_NAME = "test";
    private ManagedKafkaAlterConfigPolicy policy;
    private Map<String, Object> configs = Map.of(
            Config.ENFORCED_VALUE_CONFIGS, "compression.type:producer,unclean.leader.election.enable:false",
            Config.MUTABLE_CONFIGS, "retention.ms,max.message.bytes,segment.bytes",
            Config.RANGE_CONFIGS, Config.DEFAULT_RANGE_CONFIGS + ",min.cleanable.dirty.ratio:0.5:0.6");

    @BeforeEach
    void setup() {
        policy = new ManagedKafkaAlterConfigPolicy();
        policy.configure(configs);
    }

    @AfterEach
    void tearDown() {
        policy.close();
    }

    @Test
    void testValidateDefaults() {
        RequestMetadata r = buildRequest();
        assertDoesNotThrow(() -> policy.validate(r));
    }
    @ParameterizedTest
    @CsvSource({
            // compression.type only allows default producer as value
            "compression.type, producer, true",
            "compression.type, gzip, false",
            "compression.type, snappy, false",
            "compression.type, lz4, false",
            "compression.type, zstd, false",
            "compression.type, uncompressed, false",
            // unclean.leader.election.enable only allows default false as value
            "unclean.leader.election.enable, false, true",
            "unclean.leader.election.enable, true, false",
    })
    void testEnforcedValueRules(String configKey, String configVal, boolean isValid) {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(configKey, configVal));
        if (isValid) {
            assertDoesNotThrow(() -> policy.validate(r));
        } else {
            assertThrows(PolicyViolationException.class, () -> policy.validate(r));
        }
    }

    @ParameterizedTest
    @CsvSource({
            // the following config cannot be updated
            "retention.ms, true",
            "message.format.version, false"
    })
    void testImmutableRules(String configKey, boolean isValid) {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(configKey, "Doesn't matter"));
        if (isValid) {
            assertDoesNotThrow(() -> policy.validate(r));
        } else {
            assertThrows(PolicyViolationException.class, () -> policy.validate(r));
        }
    }

    @ParameterizedTest
    @CsvSource({
            // max.message.bytes allows value less than or equal to 1048588
            "max.message.bytes, 1048588, true",
            "max.message.bytes, 0, true",
            "max.message.bytes, 1048589, false",
            // max.message.bytes allows value greater than or equal to 52428800
            "segment.bytes, 52428800, true",
            "segment.bytes, 52428801, true",
            "segment.bytes, 0, false",
            // max.message.bytes allows value between 0.5 and 0.6
            "min.cleanable.dirty.ratio, 0.6, true",
            "min.cleanable.dirty.ratio, 0.5, true",
            "min.cleanable.dirty.ratio, 0.4, false",
            "min.cleanable.dirty.ratio, 0.7, false",
    })
    void testRangeRules(String configKey, String configVal, boolean isValid) {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(configKey, configVal));
        if (isValid) {
            assertDoesNotThrow(() -> policy.validate(r));
        } else {
            assertThrows(PolicyViolationException.class, () -> policy.validate(r));
        }
    }

    private RequestMetadata buildRequest() {
        RequestMetadata r = Mockito.mock(RequestMetadata.class);
        Mockito.when(r.resource()).thenReturn(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME));
        return r;
    }

}
