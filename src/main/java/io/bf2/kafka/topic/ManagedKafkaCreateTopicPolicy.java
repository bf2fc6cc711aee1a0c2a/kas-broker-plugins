package io.bf2.kafka.topic;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ManagedKafkaCreateTopicPolicy implements CreateTopicPolicy {
    protected static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";
    protected static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
    protected static final String MAX_PARTITONS = "max.partitions";
    private Map<String, ?> configs;

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        validateReplicationFactor(requestMetadata);
        validateIsr(requestMetadata);
        validateNumPartitions(requestMetadata);
    }

    private void validateReplicationFactor(RequestMetadata requestMetadata) throws PolicyViolationException {
        Optional<Short> defaultReplicationFactor = defaultReplicationFactor();

        // only allow replication factor if it matches to default
        if (requestMetadata.replicationFactor() != null
                && defaultReplicationFactor.isPresent()
                && !requestMetadata.replicationFactor().equals(defaultReplicationFactor.get())) {
            throw new PolicyViolationException(String.format("Topic %s configured with invalid replication factor %d, required replication factor is %d", requestMetadata.topic(), requestMetadata.replicationFactor(), defaultReplicationFactor.get()));
        }
    }

    private void validateIsr(RequestMetadata requestMetadata) throws PolicyViolationException {
        Optional<Short> defaultIsr = defaultIsr();

        // grab the client's isr value if present
        Optional<Short> isr = getConfig(MIN_INSYNC_REPLICAS, requestMetadata.configs())
                .map(v -> Short.valueOf(v.toString()));

        // if not present, cluster default value taken automatically, otherwise
        // only allow isr if the defined value >= to default isr value configured and <= system replication
        // factor, as there is no meaning setting this value higher than replication factor.
        if(isr.isPresent() && defaultIsr.isPresent()) {
            if (isr.get() < defaultIsr.get() || isr.get() > defaultReplicationFactor().get()) {
                throw new PolicyViolationException(String.format("Topic %s configured with invalid minimum insync replicas %d, recommended minimum insync replicas are %d", requestMetadata.topic(), isr.get(), defaultIsr.get()));
            }
        }
    }

    private void validateNumPartitions(RequestMetadata requestMetadata) throws PolicyViolationException {
        long maxPartitions = getConfig(MAX_PARTITONS, configs)
                .map(v -> Long.valueOf(v.toString()))
                .orElse(-1L);

        if (maxPartitions < 1) {
            return;
        }

        Integer addPartitions = Optional.ofNullable(requestMetadata.replicasAssignments())
                .map(Map::size)
                .orElseGet(requestMetadata::numPartitions);

        if (addPartitions > maxPartitions) {
            throw new PolicyViolationException(String.format("Topic %s with %d partitions exceeds the cluster partition limit of %d",
                    requestMetadata.topic(), addPartitions, maxPartitions));
        }

        try (Admin admin = Admin.create(Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9096"))) {
            List<String> topicNames = admin.listTopics(new ListTopicsOptions().listInternal(false))
                .listings()
                .get()
                .stream()
                .map(TopicListing::name)
                .filter(name -> !name.startsWith("__redhat_"))
                .collect(Collectors.toList());

            long usedPartitions = admin.describeTopics(topicNames)
                .all()
                .get()
                .values()
                .stream()
                .map(description -> (long) description.partitions().size())
                .reduce(0L, Long::sum);

            if (usedPartitions + addPartitions > maxPartitions) {
                throw new PolicyViolationException(String.format("Creating topic %s with %d partitions exceeds the cluster partition limit of %d",
                        requestMetadata.topic(), addPartitions, maxPartitions));
            }
        } catch (InterruptedException | ExecutionException e) {

        }
    }

    private Optional<Short> defaultIsr() {
        return getConfig(MIN_INSYNC_REPLICAS, this.configs)
                .map(v -> Short.valueOf(v.toString()));
    }

    private Optional<Short> defaultReplicationFactor() {
        return getConfig(DEFAULT_REPLICATION_FACTOR, this.configs)
                .map(v -> Short.valueOf(v.toString()));
    }

    private Optional<Object> getConfig(String name, Map<String, ?> configs){
        return Optional.ofNullable(configs.get(name));
    }
}
