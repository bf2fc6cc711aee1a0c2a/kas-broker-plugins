package io.bf2.kafka.common;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertSame;
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
        Map<String, ?> config = configWith(Map.of(PartitionCounter.MAX_PARTITIONS, "1000"));

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
        config.put(PartitionCounter.MAX_PARTITIONS, null);
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
        Map<String, ?> config = configWith(Map.of());
        try (PartitionCounter a = PartitionCounter.create(config);
                PartitionCounter b = PartitionCounter.create(config)) {
            assertSame(a, b);
        }
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
