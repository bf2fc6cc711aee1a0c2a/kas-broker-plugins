package io.bf2.kafka.topic;

import com.google.common.collect.ImmutableMap;
import io.bf2.kafka.common.Config;
import io.bf2.kafka.common.LocalAdminClient;
import io.bf2.kafka.common.PartitionCounter;
import io.bf2.kafka.config.ManagedKafkaAlterConfigPolicy;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


class ManagedKafkaCreateTopicPolicyTest {
    private ManagedKafkaCreateTopicPolicy policy;
    private ManagedKafkaCreateTopicPolicy disabledPolicy;
    private Map<String, Object> disabledConfigs = ImmutableMap.<String, Object>builder()
            .put(ManagedKafkaCreateTopicPolicy.DEFAULT_REPLICATION_FACTOR, 3)
            .put(MIN_IN_SYNC_REPLICAS_CONFIG, 2)
            .put(Config.MAX_PARTITIONS, 1000)
            .put(Config.LIMIT_ENFORCED, true)
            .put(Config.PRIVATE_TOPIC_PREFIX, "__kas_")
            .put(LocalAdminClient.LISTENER_NAME, "controlplane")
            .put(LocalAdminClient.LISTENER_PORT, "9090")
            .put(LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT")
            .put(Config.ENFORCED_VALUE_CONFIGS, "compression.type:producer,unclean.leader.election.enable:false")
            .put(Config.MUTABLE_CONFIGS, "min.insync.replicas,retention.ms,max.message.bytes,segment.bytes")
            .put(Config.RANGE_CONFIGS, Config.DEFAULT_RANGE_CONFIGS + ",min.cleanable.dirty.ratio:0.5:0.6")
            .build();
    private Map<String, Object> configs = ImmutableMap.<String, Object>builder()
            .putAll(disabledConfigs)
            .put(Config.TOPIC_CONFIG_POLICY_ENFORCED, true)
            .build();

    @BeforeEach
    void setup() {
        policy = new ManagedKafkaCreateTopicPolicy();
        disabledPolicy = new ManagedKafkaCreateTopicPolicy();
        policy.configure(configs);
        disabledPolicy.configure(disabledConfigs);
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
        // since policy is disabled, we should not throw exception no matter what config provided
        assertDoesNotThrow(() -> disabledPolicy.validate(r));
    }

    @ParameterizedTest
    @CsvSource({
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
        // since policy is disabled, we should not throw exception no matter what config provided
        assertDoesNotThrow(() -> disabledPolicy.validate(r));
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
        // since policy is disabled, we should not throw exception no matter what config provided
        assertDoesNotThrow(() -> disabledPolicy.validate(r));
    }

    @Test
    void testInvalidRF() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.replicationFactor()).thenReturn((short)2);
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testWhenIsrIsOne() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, "1"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testIsrGreaterThanDefault() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, "10"));
        assertThrows(PolicyViolationException.class, () -> policy.validate(r));
    }

    @Test
    void testIsrSameAsDefault() {
        RequestMetadata r = buildRequest();
        Mockito.when(r.configs()).thenReturn(Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, "2"));
        assertDoesNotThrow(() -> policy.validate(r));
    }

    @Test
    void testCanCreateTopicWithReasonablePartitions() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(0, true, true);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 100, (short) 3, null, Map.of());
            assertDoesNotThrow(() -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateTopicWithTooManyPartitions() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(0, false, true);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 1001, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateSecondTopicIfItViolates() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(998, false, true);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(998, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateSecondTopicIfLimitReached() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, false, true);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(1001, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testUsingReplicaAssignments() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(999, false, true);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(999, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata =
                    new RequestMetadata("test2", null, (short) 3, Map.of(0, List.of(0), 1, List.of(0)), Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }


    @ParameterizedTest
    @CsvSource({
            "null, ALLOWED",
            "true, DENIED",
            "false, ALLOWED"
    })
    void testPartitionLimitEnforcementFeatureFlag(String featureFlag, String expectedResult) throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, false, Boolean.parseBoolean(featureFlag));
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            Map<String, Object> customConfig = new HashMap<>(configs);

            if (!"null".equalsIgnoreCase(featureFlag)) {
                customConfig.put(Config.LIMIT_ENFORCED, featureFlag);
            }

            policy.configure(customConfig);

            RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());

            if ("DENIED".equals(expectedResult)) {
                assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
            } else {
                assertDoesNotThrow(() -> policy.validate(ctpRequestMetadata));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "topic1, 10, 1, 2, DENIED",
        "topic1, 10, 3, 1, DENIED",
        "topic1, 9999, 3, 2, DENIED",

        "__kas_topic1, 10, 1, 2, ALLOWED",
        "__kas_topic1, 10, 3, 1, ALLOWED",
        "__kas_topic1, 9999, 3, 2, ALLOWED",
    })
    void testTopicValidationBypass(String topicName, int partitions, short replicationFactor, int isr,
            String expectedResult) throws Exception {
        RequestMetadata ctpRequestMetadata = new RequestMetadata(topicName, partitions, replicationFactor, null,
                Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(isr)));

        String message = String.format("Test topic name: %s, partitions: %d, replicationFactor: %d, isr: %d, expectedResult %s", topicName, partitions, replicationFactor, isr, expectedResult);
        if ("DENIED".equals(expectedResult)) {
            assertThrows(message, PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        } else {
            assertDoesNotThrow(() -> policy.validate(ctpRequestMetadata), message);
        }
    }

    private PartitionCounter generateMockPartitionCounter(int numPartitions, boolean response, boolean limitEnforced)
            throws InterruptedException, ExecutionException, TimeoutException {
        PartitionCounter partitionCounter = Mockito.mock(PartitionCounter.class);
        Mockito.when(partitionCounter.getMaxPartitions()).thenReturn(1000);
        Mockito.when(partitionCounter.getExistingPartitionCount()).thenReturn(numPartitions);
        Mockito.when(partitionCounter.countExistingPartitions()).thenReturn(numPartitions);
        Mockito.when(partitionCounter.reservePartitions(Mockito.anyInt())).thenReturn(response);
        Mockito.when(partitionCounter.isLimitEnforced()).thenReturn(limitEnforced);

        return partitionCounter;
    }

    private RequestMetadata buildRequest() {
        RequestMetadata r = Mockito.mock(RequestMetadata.class);
        Mockito.when(r.topic()).thenReturn("test");
        Mockito.when(r.replicationFactor()).thenReturn((short)3);
        return r;
    }

}
