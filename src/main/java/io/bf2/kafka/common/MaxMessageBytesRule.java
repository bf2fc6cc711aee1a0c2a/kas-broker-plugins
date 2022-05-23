package io.bf2.kafka.common;

import java.util.Map;

import static org.apache.kafka.common.config.TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG;

/**
 * This is a rule that only allow configs using default value
 */
public class MaxMessageBytesRule implements ConfigRule{
    @Override
    public boolean IsValid(Map<String, String> configs) {
        return !configs.containsKey(MAX_MESSAGE_BYTES_CONFIG) ||
                (configs.containsKey(MAX_MESSAGE_BYTES_CONFIG) && Integer.parseInt(configs.get(MAX_MESSAGE_BYTES_CONFIG)) <= 1048588);
    }
}
