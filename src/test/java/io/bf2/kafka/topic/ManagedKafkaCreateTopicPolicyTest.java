package io.bf2.kafka.topic;

import io.bf2.kafka.common.LocalAdminClient;
import io.bf2.kafka.common.PartitionCounter;
import io.bf2.kafka.common.PartitionCounter.ReservationResponse;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;


class ManagedKafkaCreateTopicPolicyTest {
    ManagedKafkaCreateTopicPolicy policy;
    Map<String, Object> configs = Map.of(
            ManagedKafkaCreateTopicPolicy.DEFAULT_REPLICATION_FACTOR, 3,
            ManagedKafkaCreateTopicPolicy.MIN_INSYNC_REPLICAS, 2,
            PartitionCounter.MAX_PARTITIONS, 1000,
            LocalAdminClient.LISTENER_NAME, "controlplane",
            LocalAdminClient.LISTENER_PORT, "9090",
            LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");

    @BeforeEach
    void setup() {
        policy = new ManagedKafkaCreateTopicPolicy();
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
        assertDoesNotThrow(() -> policy.validate(r));
    }

    @Test
    void testCanCreateTopicWithReasonablePartitions() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(0, ReservationResponse.SUCCEEDED);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 100, (short) 3, null, Map.of());
            assertDoesNotThrow(() -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateTopicWithTooManyPartitions() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(0, ReservationResponse.REJECTED);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 1001, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateSecondTopicIfItViolates() throws Exception {

        PartitionCounter partitionCounter = generateMockPartitionCounter(998, ReservationResponse.REJECTED);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(998, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testCantCreateSecondTopicIfLimitReached() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, ReservationResponse.REJECTED);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(1001, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    @Test
    void testUsingReplicaAssignments() throws Exception {
        PartitionCounter partitionCounter = generateMockPartitionCounter(999, ReservationResponse.REJECTED);
        try (ManagedKafkaCreateTopicPolicy policy = new ManagedKafkaCreateTopicPolicy(partitionCounter)) {
            policy.configure(configs);
            assertEquals(999, partitionCounter.getExistingPartitionCount());

            RequestMetadata ctpRequestMetadata =
                    new RequestMetadata("test2", null, (short) 3, Map.of(0, List.of(0), 1, List.of(0)), Map.of());
            assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
        }
    }

    private PartitionCounter generateMockPartitionCounter(int numPartitions, ReservationResponse reservationResponse) {
        PartitionCounter partitionCounter = Mockito.mock(PartitionCounter.class);
        Mockito.when(partitionCounter.getMaxPartitions()).thenReturn(1000);
        Mockito.when(partitionCounter.getExistingPartitionCount()).thenReturn(numPartitions);
        Mockito.when(partitionCounter.reservePartitions(Mockito.anyInt())).thenReturn(reservationResponse);

        return partitionCounter;
    }

    private RequestMetadata buildRequest() {
        RequestMetadata r = Mockito.mock(RequestMetadata.class);
        Mockito.when(r.topic()).thenReturn("test");
        Mockito.when(r.replicationFactor()).thenReturn((short)3);
        return r;
    }

}
