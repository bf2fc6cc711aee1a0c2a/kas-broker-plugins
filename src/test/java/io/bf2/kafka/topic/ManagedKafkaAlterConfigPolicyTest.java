package io.bf2.kafka.topic;

import io.bf2.kafka.common.LocalAdminClient;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static io.bf2.kafka.common.ConfigRules.DEFAULT_MAX_MESSAGE_BYTE_ALLOWED;
import static io.bf2.kafka.common.ConfigRules.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.*;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;


class ManagedKafkaAlterConfigPolicyTest {
    private static final String TOPIC_NAME = "test";
    private ManagedKafkaAlterConfigPolicy policy;
    private Map<String, Object> configs = Map.of(
            LocalAdminClient.LISTENER_NAME, "controlplane",
            LocalAdminClient.LISTENER_PORT, "9090",
            LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");

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

//    @Test
//    void testInvalidCompressionType() {
//        RequestMetadata r = buildRequest();
//        Mockito.when(r.configs()).thenReturn(Map.of(COMPRESSION_TYPE_CONFIG, "gzip"));
//        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
//    }
//
//    @Test
//    void testValidFileDeleteDelayMs() {
//        RequestMetadata r = buildRequest();
//        Mockito.when(r.configs()).thenReturn(Map.of(FILE_DELETE_DELAY_MS_CONFIG, "60000"));
//        assertDoesNotThrow(() -> policy.validate(r));
//    }

    @Test
    void testInvalidMaxMessageBytes() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MAX_MESSAGE_BYTES_CONFIG, String.valueOf(Integer.parseInt(DEFAULT_MAX_MESSAGE_BYTE_ALLOWED) + 1)));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testValidMaxMessageBytes() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MAX_MESSAGE_BYTES_CONFIG, String.valueOf(Integer.parseInt(DEFAULT_MAX_MESSAGE_BYTE_ALLOWED) - 1)));
        assertDoesNotThrow(() -> policy.validate(r));
    }

    @Test
    void testInvalidMinInsyncReplicas() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, "3"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testInvalidLeaderReplicationThrottledReplicas() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG, "*"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

//    @Test
//    void testIsrSameAsDefault() {
//        RequestMetadata r = buildRequest();
//        Mockito.when(r.configs()).thenReturn(Map.of(ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, "2"));
//        assertDoesNotThrow(() -> policy.validate(r));
//    }

    private RequestMetadata buildRequest() {
        RequestMetadata r = Mockito.mock(RequestMetadata.class);
        Mockito.when(r.resource()).thenReturn(new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME));
        return r;
    }

}
