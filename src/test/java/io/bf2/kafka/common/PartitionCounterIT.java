package io.bf2.kafka.common;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@ExtendWith(KafkaJunitExtension.class)
@KafkaJunitExtensionConfig(startupMode = StartupMode.WAIT_FOR_STARTUP)
class PartitionCounterIT {

        @Test
        void testOnlyCountPublicTopicPartitions(KafkaHelper kafkaHelper)
                throws InterruptedException, ExecutionException, TimeoutException {
            try (Admin admin = Admin.create(kafkaHelper.consumerConfig())) {
                final int PUBLIC_PARTITION_COUNT = 20;
                final String customPrivateTopicPrefix = "__kas_";
                List<NewTopic> newTopics = List.of(
                        new NewTopic("topic1", PUBLIC_PARTITION_COUNT / 2, (short) 1),
                        new NewTopic(Config.DEFAULT_NO_PRIVATE_TOPIC_PREFIX + "topic", PUBLIC_PARTITION_COUNT / 2, (short) 1),
                        new NewTopic(customPrivateTopicPrefix + "topic", 11, (short) 1),
                        new NewTopic("__consumer_offsets", 12, (short) 1),
                        new NewTopic("__transaction_state", 13, (short) 1));
                CreateTopicsResult result = admin.createTopics(newTopics);
                Futures.getUnchecked(result.all());

                Map<String, Object> config = Stream.concat(
                        kafkaHelper.consumerConfig().entrySet().stream(),
                        Map.of(
                                Config.PRIVATE_TOPIC_PREFIX, "__kas_",
                                LocalAdminClient.LISTENER_NAME, "test",
                                LocalAdminClient.LISTENER_PORT, kafkaHelper.kafkaPort(),
                                LocalAdminClient.LISTENER_PROTOCOL, "PLAINTEXT").entrySet().stream())
                        .collect(Collectors.toMap(e -> e.getKey().toString(), Entry::getValue));
                try (PartitionCounter partitionCounter = PartitionCounter.create(config)) {
                    await().atMost(Config.DEFAULT_SCHEDULE_INTERVAL_SECONDS * 2, TimeUnit.SECONDS)
                            .until(() -> partitionCounter.getExistingPartitionCount() == PUBLIC_PARTITION_COUNT);
                }
            }
        }
}
