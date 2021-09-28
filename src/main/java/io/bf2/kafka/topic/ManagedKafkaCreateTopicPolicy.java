package io.bf2.kafka.topic;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

public class ManagedKafkaCreateTopicPolicy implements CreateTopicPolicy {
    protected static final String DEFAULT_REPLICATION_FACTOR = "default.replication.factor";
    protected static final String MIN_INSYNC_REPLICAS = "min.insync.replicas";
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
