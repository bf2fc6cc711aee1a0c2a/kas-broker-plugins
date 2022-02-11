package io.bf2.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class PartitionCounterTest {

    @Test
    void testGetMaxPartitionsHappy() {
        Map<String, ?> config = Map.of(PartitionCounter.MAX_PARTITONS, 1000);
        assertEquals(1000, PartitionCounter.getMaxPartitions(config));
    }

    @Test
    void testGetMaxPartitionsHappyString() {
        Map<String, ?> config = Map.of(PartitionCounter.MAX_PARTITONS, "1000");
        assertEquals(1000, PartitionCounter.getMaxPartitions(config));
    }

    @Test
    void testGetMaxPartitionsInvalidString() {
        Map<String, ?> config = Map.of(PartitionCounter.MAX_PARTITONS, "ten");
        assertEquals(-1, PartitionCounter.getMaxPartitions(config));
    }

    @Test
    void testGetMaxPartitionsNull() {
        Map<String, ?> config = new HashMap<>();
        config.put(PartitionCounter.MAX_PARTITONS, null);
        assertEquals(-1, PartitionCounter.getMaxPartitions(config));
    }

    @Test
    void testGetMaxPartitionsNonExistent() {
        Map<String, ?> config = Map.of();
        assertEquals(-1, PartitionCounter.getMaxPartitions(config));
    }
}
