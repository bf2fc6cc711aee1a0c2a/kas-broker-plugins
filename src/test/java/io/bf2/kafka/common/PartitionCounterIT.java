package io.bf2.kafka.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.charithe.kafka.KafkaHelper;
import com.github.charithe.kafka.KafkaJunitExtension;
import com.github.charithe.kafka.KafkaJunitExtensionConfig;
import com.github.charithe.kafka.StartupMode;
import com.google.common.util.concurrent.Futures;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

class PartitionCounterIT {

    @Nested
    @ExtendWith(KafkaJunitExtension.class)
    @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
    class BasicTest {
        @Test
        void testCountExistingPartitions(KafkaHelper kafkaHelper) throws InterruptedException, ExecutionException {
            try (Admin admin = Admin.create(kafkaHelper.consumerConfig())) {
                List<NewTopic> newTopics = List.of(
                        new NewTopic("topic1", 10, (short) 1),
                        new NewTopic("topic2", 10, (short) 1));
                CreateTopicsResult result = admin.createTopics(newTopics);
                Futures.getUnchecked(result.all());

                assertEquals(20, PartitionCounter.countExistingPartitions(admin));
            }
        }
    }

    @Nested
    @ExtendWith(KafkaJunitExtension.class)
    @KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
    class InternalPartitionsTest {
        @Test
        void testOmitsInternalPartitions(KafkaHelper kafkaHelper)
                throws InterruptedException, ExecutionException {
            try (Admin admin = Admin.create(kafkaHelper.consumerConfig())) {
                List<NewTopic> newTopics = List.of(
                        new NewTopic("topic3", 10, (short) 1),
                        new NewTopic("__redhat_topic", 11, (short) 1),
                        new NewTopic("__consumer_offsets", 12, (short) 1));
                CreateTopicsResult result = admin.createTopics(newTopics);
                Futures.getUnchecked(result.all());

                assertEquals(10, PartitionCounter.countExistingPartitions(admin));
            }
        }
    }

}
