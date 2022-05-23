package io.bf2.kafka.common;

import java.util.Map;

import static org.apache.kafka.common.config.TopicConfig.*;

/**
 * This is a rule that only allow configs using default value
 */
public class DefaultConfigValueRule implements ConfigRule{
//    private static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "follower.replication.throttled.replicas";
//    private static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG = "leader.replication.throttled.replicas";
    private static final Map<String, String> CONFIG_WITH_DEFAULT_VALUE = Map.of(
            COMPRESSION_TYPE_CONFIG, "producer",
            FILE_DELETE_DELAY_MS_CONFIG, String.valueOf(60000),
            FLUSH_MESSAGES_INTERVAL_CONFIG, String.valueOf(9223372036854775807L),
            FLUSH_MS_CONFIG, String.valueOf(9223372036854775807L),
            INDEX_INTERVAL_BYTES_CONFIG, String.valueOf(4096),
            MIN_CLEANABLE_DIRTY_RATIO_CONFIG, String.valueOf(0.5),
            PREALLOCATE_CONFIG, String.valueOf(false),
            SEGMENT_JITTER_MS_CONFIG, String.valueOf(0),
            UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, String.valueOf(false)
    );


    @Override
    public boolean IsValid(Map<String, String> configs) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (CONFIG_WITH_DEFAULT_VALUE.containsKey(key)) {
                if (!entry.getValue().equals(CONFIG_WITH_DEFAULT_VALUE.get(key))) {
                    return false;
                }
            }
        }
        return true;
    }
}
