package io.bf2.kafka.topic;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaHelper;
import com.google.common.util.concurrent.Futures;
import io.bf2.kafka.common.LocalAdminClient;
import io.bf2.kafka.common.PartitionCounter;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ManagedKafkaCreateTopicPolicyIT {

    Admin admin;
    Map<String, Object> config;
    EphemeralKafkaBroker broker;
    PartitionCounter partitionCounter;
    ManagedKafkaCreateTopicPolicy policy;

    @BeforeEach
    void initialize() throws Exception {
        config = getConfig();

        broker = getBrokerInstance(config);
        Futures.getUnchecked(broker.start());

        admin = LocalAdminClient.create(config);
        KafkaHelper kafkaHelper = KafkaHelper.createFor(broker);
        Map<String, Object> configs = Stream.concat(kafkaHelper.consumerConfig()
                .entrySet()
                .stream(),
                config.entrySet().stream())
                .collect(Collectors.toMap(e -> e.getKey().toString(), Entry::getValue));
        partitionCounter = new PartitionCounter(configs);

        policy = new ManagedKafkaCreateTopicPolicy();
    }

    @AfterEach
    void close() throws Exception {
        admin.close();
        broker.stop();
        policy.close();
        partitionCounter.close();
    }

    @Test
    void testCanCreateTopicWithReasonablePartitions() throws Exception {
            policy.configure(config);
            RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 100, (short) 3, null, Map.of());
            policy.validate(ctpRequestMetadata);
    }

    @Test
    void testCantCreateTopicWithTooManyPartitions() throws Exception {
        policy.configure(config);
        RequestMetadata ctpRequestMetadata = new RequestMetadata("test1", 1001, (short) 3, null, Map.of());
        assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
    }

    @Test
    void testCantCreateSecondTopicIfItViolates() throws Exception {
        policy.configure(config);

        admin.createTopics(List.of(new NewTopic("test1", Optional.of(998), Optional.empty())));
        Thread.sleep(PartitionCounter.DEFAULT_SCHEDULE_PERIOD_MILLIS * 2);
        assertEquals(998, partitionCounter.countExistingPartitions());

        RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
        assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
    }

    @Test
    void testCantCreateSecondTopicIfLimitReached() throws Exception {
        policy.configure(config);

        admin.createTopics(List.of(new NewTopic("test1", Optional.of(1001), Optional.empty())));
        Thread.sleep(PartitionCounter.DEFAULT_SCHEDULE_PERIOD_MILLIS * 2);
        assertEquals(1001, partitionCounter.countExistingPartitions());

        RequestMetadata ctpRequestMetadata = new RequestMetadata("test2", 3, (short) 3, null, Map.of());
        assertThrows(PolicyViolationException.class, () -> policy.validate(ctpRequestMetadata));
    }

    public static EphemeralKafkaBroker getBrokerInstance(Map<String, Object> config) {
        Properties properties = new Properties();
        properties.putAll(config);

        return EphemeralKafkaBroker.create(
                (int) config.get("strimzi.authorization.custom-authorizer.adminclient-listener.port"), -1, properties);
    }

    public static Map<String, Object> getConfig() {
        return Map.of(
                "max.partitions", 1000,
                "strimzi.authorization.custom-authorizer.adminclient-listener.name", "controlplane",
                "strimzi.authorization.custom-authorizer.adminclient-listener.port", InstanceSpec.getRandomPort(),
                "strimzi.authorization.custom-authorizer.adminclient-listener.protocol", "PLAINTEXT");
    }
}
