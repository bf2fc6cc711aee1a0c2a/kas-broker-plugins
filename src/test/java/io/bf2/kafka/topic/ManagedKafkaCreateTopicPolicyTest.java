package io.bf2.kafka.topic;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;


class ManagedKafkaCreateTopicPolicyTest {
    ManagedKafkaCreateTopicPolicy policy;

    @BeforeEach
    void setup() {
        policy = new ManagedKafkaCreateTopicPolicy();
        final Map<String, Object> configs = Map.of(
                ManagedKafkaCreateTopicPolicy.DEFAULT_REPLICATION_FACTOR, 3,
                ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, 2,
                "strimzi.authorization.custom-authorizer.adminclient-listener.name", "controlplane",
                "strimzi.authorization.custom-authorizer.adminclient-listener.port", "9090",
                "strimzi.authorization.custom-authorizer.adminclient-listener.protocol", "PLAINTEXT");
        policy.configure(configs);
    }

    @AfterEach
    void tearDown() {
        policy.close();
    }

    @Test
    void testValidateDefaults() {
        RequestMetadata r = buildRequest();
        policy.validate(r);
    }

    @Test
    void testInValidRF() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.replicationFactor()).thenReturn((short)2);
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testWhenIsrIsOne() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, "1"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testIsrGreaterThanDefault() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, "10"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testIsrSameAsDefault() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, "2"));
        policy.validate(r);
    }

    private RequestMetadata buildRequest() {
        RequestMetadata r = Mockito.mock(RequestMetadata.class);
        Mockito.when(r.topic()).thenReturn("test");
        Mockito.when(r.replicationFactor()).thenReturn((short)3);
        return r;
    }
}
