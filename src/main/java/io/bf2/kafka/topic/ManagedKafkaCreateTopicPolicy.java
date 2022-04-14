/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.topic;

import io.bf2.kafka.common.PartitionCounter;
import io.bf2.kafka.common.PartitionLimitEnforcement;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class ManagedKafkaCreateTopicPolicy implements CreateTopicPolicy {
    protected static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";
    protected static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";

    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaCreateTopicPolicy.class);

    private volatile Map<String, ?> configs;
    private volatile PartitionCounter partitionCounter;
    private volatile boolean partitionLimitEnforcementEnabled = true;

    public ManagedKafkaCreateTopicPolicy() {
    }

    ManagedKafkaCreateTopicPolicy(PartitionCounter partitionCounter) {
        this();
        this.partitionCounter = partitionCounter;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;

        if (partitionCounter == null) {
            partitionCounter = PartitionCounter.create(configs);
        }

        partitionLimitEnforcementEnabled = PartitionLimitEnforcement.isEnabled(configs);
    }

    @Override
    public void close() {
        if (partitionCounter != null) {
            partitionCounter.close();
        }
    }

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        validateReplicationFactor(requestMetadata);
        validateIsr(requestMetadata);

        if (partitionLimitEnforcementEnabled) {
            validateNumPartitions(requestMetadata);
        }
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
        if ((isr.isPresent() && defaultIsr.isPresent()) && (isr.get() < defaultIsr.get() || isr.get() > defaultReplicationFactor().get())) {
            throw new PolicyViolationException(String.format("Topic %s configured with invalid minimum insync replicas %d, recommended minimum insync replicas are %d", requestMetadata.topic(), isr.get(), defaultIsr.get()));
        }
    }

    private void validateNumPartitions(RequestMetadata requestMetadata) throws PolicyViolationException {
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

    private String policyViolationExceptionMessage(String topic, int partitions, int maxPartitions, int remainingBudget) {
        return String.format(
                "Topic %s with %d partitions would exceed the cluster partition limit of %d (current count is %d)",
                topic, partitions, maxPartitions, maxPartitions - remainingBudget);
    }
}
