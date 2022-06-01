package io.bf2.kafka.topic;

import io.bf2.kafka.common.ConfigRules;
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
            LocalAdminClient.LISTENER_NAME, "controlplane",
            LocalAdminClient.LISTENER_PORT, "9090",
            LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT",
            ConfigRules.ALLOW_ONE_CONFIG_VALUE_CONFIGS, "compression.type:producer,unclean.leader.election.enable:false",
            ConfigRules.NOT_ALLOW_UPDATE_CONFIGS, "message.format.version");

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
    void testDefaultConfigValueRules(String configKey, String configVal, boolean isValid) {
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
            "message.format.version"
    })
    void testNotAllowUpdateConfigValueRules(String configKey) {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(configKey, "Doesn't matter"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @ParameterizedTest
    @CsvSource({
            // max.message.bytes only allows value less than or equal to 1048588
            "max.message.bytes, 1048588, true",
            "max.message.bytes, 0, true",
            "max.message.bytes, 1048589, false"
    })
    void testLessThanAndEqualToConfigValueRules(String configKey, String configVal, boolean isValid) {
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
