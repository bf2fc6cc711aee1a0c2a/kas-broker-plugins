package io.bf2.kafka.common;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.*;

/**
 * This is a rule that only allow configs using default value
 */
public class NotAllowUpdateRule implements ConfigRule{
    private static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
    private static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
    private static final Set<String> CONFIG_CANNOT_UPDATE = Set.of(
            MESSAGE_FORMAT_VERSION_CONFIG,
            FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG,
            LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG
    );


    @Override
    public boolean IsValid(Map<String, String> configs) {
        for (String key : configs.keySet()) {
            if (CONFIG_CANNOT_UPDATE.contains(key)) {
                return false;
            }
        }
        return true;
    }
}
