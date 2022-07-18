package io.bf2.kafka.topic;

import io.bf2.kafka.common.Config;
import io.bf2.kafka.common.PartitionCounter;
import io.bf2.kafka.common.rule.ConfigRules;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ManagedKafkaCreateTopicPolicy implements CreateTopicPolicy {
    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaCreateTopicPolicy.class);

    private volatile Map<String, ?> configs;
    private volatile PartitionCounter partitionCounter;
    private ConfigRules configRules;

    public ManagedKafkaCreateTopicPolicy() {
    }

    ManagedKafkaCreateTopicPolicy(PartitionCounter partitionCounter) {
        this();
        this.partitionCounter = partitionCounter;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        this.configRules = new ConfigRules(configs);

        if (partitionCounter == null) {
            partitionCounter = PartitionCounter.create(configs);
        }
    }

    @Override
    public void close() {
        if (partitionCounter != null) {
            partitionCounter.close();
        }
    }

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (partitionCounter.isInternalTopic(requestMetadata.topic())) {
            return;
        }

        validateReplicationFactor(requestMetadata);
        validateNumPartitions(requestMetadata);
        validateConfigs(requestMetadata);
    }

    private void validateConfigs(RequestMetadata requestMetadata) throws PolicyViolationException {
        Map<String, String> configs = requestMetadata.configs();
        if (!configs.isEmpty()) {
            configRules.validateTopicConfigs(requestMetadata.topic(), configs);
        }
    }

    private void validateReplicationFactor(RequestMetadata requestMetadata) throws PolicyViolationException {
        Optional<Short> defaultReplicationFactor = Config.brokerDefaultReplicationFactor(this.configs);

        // only allow replication factor if it matches to default
        if (requestMetadata.replicationFactor() != null
                && defaultReplicationFactor.isPresent()
                && !requestMetadata.replicationFactor().equals(defaultReplicationFactor.get())) {
            throw new PolicyViolationException(String.format("Topic %s configured with invalid replication factor %d, required replication factor is %d", requestMetadata.topic(), requestMetadata.replicationFactor(), defaultReplicationFactor.get()));
        }
    }

    private void validateNumPartitions(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (!partitionCounter.isLimitEnforced()) {
            return;
        }

        int maxPartitions = partitionCounter.getMaxPartitions();
        if (maxPartitions < 1) {
            return;
        }

        partitionCounter.getExistingPartitionCount();

        Integer addPartitions = Optional.ofNullable(requestMetadata.replicasAssignments())
                .map(Map::size)
                .orElseGet(() -> requestMetadata.numPartitions() != null ? requestMetadata.numPartitions() : 0);

        if (addPartitions > maxPartitions) {
            throw new PolicyViolationException(policyViolationExceptionMessage(requestMetadata.topic(), addPartitions,
                    maxPartitions, partitionCounter.getRemainingPartitionBudget()));
        }

        if (!partitionCounter.reservePartitions(addPartitions)) {
            try {
                int recheckedPartitionCount = partitionCounter.countExistingPartitions();
                if (addPartitions + recheckedPartitionCount > maxPartitions) {
                    throw new PolicyViolationException(policyViolationExceptionMessage(requestMetadata.topic(),
                            addPartitions, maxPartitions, maxPartitions - recheckedPartitionCount));
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error("Exception occurred while performing synchronous partition recount", e);
                throw new PolicyViolationException(policyViolationExceptionMessage(requestMetadata.topic(),
                        addPartitions, maxPartitions, partitionCounter.getRemainingPartitionBudget()), e);
            }
        }
    }



    private String policyViolationExceptionMessage(String topic, int partitions, int maxPartitions, int remainingBudget) {
        return String.format(
                "Topic %s with %d partitions would exceed the cluster partition limit of %d (current count is %d)",
                topic, partitions, maxPartitions, maxPartitions - remainingBudget);
    }
}
