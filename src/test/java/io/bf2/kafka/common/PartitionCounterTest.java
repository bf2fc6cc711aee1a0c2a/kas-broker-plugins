package io.bf2.kafka.common;

import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PartitionCounterTest {

    @Test
    void testGetMaxPartitionsHappy() {
        Map<String, ?> config = configWith(Map.of());
        PartitionCounter partitionCounter = new PartitionCounter(config);
        assertEquals(1000, partitionCounter.getMaxPartitions());
        partitionCounter.close();
    }

    @Test
    void testGetMaxPartitionsHappyString() {
        Map<String, ?> config = configWith(Map.of("max.partitions", "1000"));

        PartitionCounter partitionCounter = new PartitionCounter(config);
        assertEquals(1000, partitionCounter.getMaxPartitions());
        partitionCounter.close();
    }

    @Test
    void testGetMaxPartitionsInvalidString() {
        Map<String, ?> config = configWith(Map.of("max.partitions", "ten"));

        PartitionCounter partitionCounter = new PartitionCounter(config);
        assertEquals(-1, partitionCounter.getMaxPartitions());
        partitionCounter.close();
    }

    @Test
    void testGetMaxPartitionsNull() {
        Map<String, ?> config = new HashMap<>(Map.of(
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT"));
        config.put(PartitionCounter.MAX_PARTITIONS, null);
        PartitionCounter partitionCounter = new PartitionCounter(config);
        assertEquals(-1, partitionCounter.getMaxPartitions());
        partitionCounter.close();
    }

    @Test
    void testGetMaxPartitionsNonExistent() {
        Map<String, ?> config = Map.of(
                //PartitionCounter.MAX_PARTITONS, 1000,
                LocalAdminClient.LISTENER_NAME, "controlplane",
                LocalAdminClient.LISTENER_PORT, "9090",
                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT");
        PartitionCounter partitionCounter = new PartitionCounter(config);
        assertEquals(-1, partitionCounter.getMaxPartitions());
        partitionCounter.close();
    }

    @Test
    void testSharedInstance() {
        Map<String, ?> config = configWith(Map.of());
        PartitionCounter a = PartitionCounter.create(config);
        PartitionCounter b = PartitionCounter.create(config);
        assertSame(a, b);
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
