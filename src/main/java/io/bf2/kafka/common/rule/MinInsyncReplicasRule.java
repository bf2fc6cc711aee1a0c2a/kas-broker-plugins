package io.bf2.kafka.common.rule;

import com.google.common.collect.Range;
import io.bf2.kafka.common.Config;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;

/**
 * This is a rule that validate min.insync.replicas config
 */
public class MinInsyncReplicasRule implements ConfigRule {
    private final Optional<Short> brokerMinInSyncReplicas;
    private final Optional<Short> brokerDefaultReplicationFactor;

    public MinInsyncReplicasRule(Optional<Short> brokerMinInSyncReplicas, Optional<Short> brokerDefaultReplicationFactor) {
        this.brokerMinInSyncReplicas = brokerMinInSyncReplicas;
        this.brokerDefaultReplicationFactor = brokerDefaultReplicationFactor;
    }

    @Override
    public Optional<String> validate(String key, String val) {
        if (key.equals(MIN_IN_SYNC_REPLICAS_CONFIG)) {
            Short minInsyncReplicas = Short.parseShort(val);
            // if not present, cluster default value taken automatically, otherwise
            // only allow isr if the defined value >= default isr value configured and <= system replication
            // factor, as there is no meaning setting this value higher than replication factor.
            if ((brokerMinInSyncReplicas.isPresent()) &&
                    (minInsyncReplicas < brokerMinInSyncReplicas.get() || minInsyncReplicas > brokerDefaultReplicationFactor.get())) {
                return Optional.of(String.format("Topic configured with invalid configs: %s=%s, recommended minimum insync replicas are %d",
                        MIN_IN_SYNC_REPLICAS_CONFIG, val, brokerMinInSyncReplicas.get()));
            }
        }
        return Optional.empty();
    }


}
