package io.bf2.kafka.common;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;

/**
 * This is a rule that only allow configs using default value
 */
public class MinInsyncReplicasRule implements ConfigRule{
    @Override
    public boolean IsValid(Map<String, String> configs) {
        return !configs.containsKey(MIN_IN_SYNC_REPLICAS_CONFIG) ||
                (configs.containsKey(MIN_IN_SYNC_REPLICAS_CONFIG) && configs.get(MIN_IN_SYNC_REPLICAS_CONFIG).equals("2"));
    }
}
