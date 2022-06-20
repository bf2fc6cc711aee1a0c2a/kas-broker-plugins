package io.bf2.kafka.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionCounterTest {

    @Test
    void testGetMaxPartitionsHappy() {
        Map<String, ?> config = configWith(Map.of());
        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertEquals(1000, partitionCounter.getMaxPartitions());
        }
    }

    @Test
    void testGetMaxPartitionsHappyString() {
        Map<String, ?> config = configWith(Map.of(Config.MAX_PARTITIONS, "1000"));

        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertEquals(1000, partitionCounter.getMaxPartitions());
        }
    }

    @Test
    void testGetMaxPartitionsNull() {
        Map<String, ?> config = new HashMap<>(Map.of(
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT"));
        config.put(Config.MAX_PARTITIONS, null);
        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertEquals(-1, partitionCounter.getMaxPartitions());
        }
    }

    @Test
    void testGetMaxPartitionsNonExistent() {
        Map<String, ?> config = Map.of(
                //PartitionCounter.MAX_PARTITONS, 1000,
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");
        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertEquals(-1, partitionCounter.getMaxPartitions());
        }
    }

    @Test
    void testSharedInstance() {
        assertEquals(0, PartitionCounter.getHandleCount());

        Map<String, ?> config = configWith(Map.of());
        PartitionCounter copy;
        try (PartitionCounter a = PartitionCounter.create(config);
             PartitionCounter b = PartitionCounter.create(config)) {
            assertEquals(2, PartitionCounter.getHandleCount());
            assertSame(a, b);
            copy = a;
        }

        assertEquals(0, PartitionCounter.getHandleCount());
        try (PartitionCounter again = PartitionCounter.create(config)) {
            assertNotSame(copy, again);
        }
    }

    @Test
    void testIsInternalTopic() {
        Map<String, ?> config = configWith(Map.of(Config.PRIVATE_TOPIC_PREFIX, "__private_"));
        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertTrue(partitionCounter.isInternalTopic("__private_topic1"));
            assertTrue(partitionCounter.isInternalTopic("__consumer_offsets"));
            assertTrue(partitionCounter.isInternalTopic("__transaction_state"));
        }
    }

    @Test
    void testIsInternalTopicWithEmptyPrivatePrefix() {
        Map<String, ?> config = configWith(Map.of(Config.PRIVATE_TOPIC_PREFIX, ""));
        try (PartitionCounter partitionCounter = new PartitionCounter(config)) {
            assertFalse(partitionCounter.isInternalTopic("__private_topic1"));
            assertTrue(partitionCounter.isInternalTopic("__consumer_offsets"));
            assertTrue(partitionCounter.isInternalTopic("__transaction_state"));
        }
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
