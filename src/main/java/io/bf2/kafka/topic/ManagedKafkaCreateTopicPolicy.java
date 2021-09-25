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
        // only allow replication factor if it matches to default
        if (requestMetadata.replicationFactor() != null && requestMetadata.replicationFactor() != defaultReplicationFactor()) {
            throw new PolicyViolationException(String.format("Topic %s configured with invalid replication factor %d, required replication factor is %d", requestMetadata.topic(), requestMetadata.replicationFactor(), defaultReplicationFactor()));
        }
    }

    private void validateIsr(RequestMetadata requestMetadata) throws PolicyViolationException {
        Short defaultIsr =  getConfig(MIN_INSYNC_REPLICAS, configs)
                .map(v -> Short.valueOf(v.toString()))
                .orElse((short)2);

        // only allow isr greater than equal to 2 or up to default
        Optional<Short> isr = getConfig(MIN_INSYNC_REPLICAS, requestMetadata.configs())
            .map(c -> Short.valueOf(c.toString()));

        if(isr.isPresent()) {
            if (isr.get() < 2 || isr.get() > defaultIsr) {
                throw new PolicyViolationException(String.format("Topic %s configured with invalid minimum insync replicas, recommended minimum insync replicas are %d", requestMetadata.topic(), defaultIsr));
            }
        }
    }

    private short defaultReplicationFactor() {
        return getConfig(DEFAULT_REPLICATION_FACTOR, this.configs)
                .map(c -> Short.valueOf(c.toString()))
                .orElse(Short.valueOf((short)3));
    }

    private Optional<Object> getConfig(String name, Map<String, ?> configs){
        return Optional.ofNullable(configs.get(name));
    }
}
