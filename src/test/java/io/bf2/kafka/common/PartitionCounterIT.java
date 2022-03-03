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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class PartitionCounterIT {

        @Test
        void testOnlyCountPublicTopicPartitions(KafkaHelper kafkaHelper)
                throws InterruptedException, ExecutionException, TimeoutException {
            try (Admin admin = Admin.create(kafkaHelper.consumerConfig())) {
                final int PUBLIC_PARTITION_COUNT = 20;
                List<NewTopic> newTopics = List.of(
                        new NewTopic("topic1", PUBLIC_PARTITION_COUNT / 2, (short) 1),
                        new NewTopic("topic2", PUBLIC_PARTITION_COUNT / 2, (short) 1),
                        new NewTopic("__redhat_topic", 11, (short) 1),
                        new NewTopic("__consumer_offsets", 12, (short) 1));
                CreateTopicsResult result = admin.createTopics(newTopics);
                Futures.getUnchecked(result.all());

                Map<String, Object> config = Stream.concat(kafkaHelper.consumerConfig()
                        .entrySet()
                        .stream(),
                        Stream.of(
                                Map.entry("strimzi.authorization.custom-authorizer.adminclient-listener.name", "test"),
                                Map.entry("strimzi.authorization.custom-authorizer.adminclient-listener.port",
                                        kafkaHelper.kafkaPort()),
                                Map.entry("strimzi.authorization.custom-authorizer.adminclient-listener.protocol",
                                        "PLAINTEXT")))
                        .collect(Collectors.toMap(e -> e.getKey().toString(), Entry::getValue));
                PartitionCounter partitionCounter = new PartitionCounter(config);
                Thread.sleep(PartitionCounter.SCHEDULE_PERIOD_MILLIS * 2);
                assertEquals(PUBLIC_PARTITION_COUNT, partitionCounter.getExistingPartitionCount());
                partitionCounter.close();
            }
        }
}
