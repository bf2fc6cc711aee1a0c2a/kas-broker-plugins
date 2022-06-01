package io.bf2.kafka.common;

import io.bf2.kafka.authorizer.AclLoggingConfig;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.config.TopicConfig.*;

/**
 * This is a rule that only allow configs using default value
 */
public class DefaultConfigValueRule implements ConfigRule {
    private static final Logger log = LoggerFactory.getLogger(DefaultConfigValueRule.class);
//    private static final Map<String, String> CONFIG_WITH_DEFAULT_VALUE = Map.of(
//            COMPRESSION_TYPE_CONFIG, "producer",
//            FILE_DELETE_DELAY_MS_CONFIG, String.valueOf(60000),
//            FLUSH_MESSAGES_INTERVAL_CONFIG, String.valueOf(9223372036854775807L),
//            FLUSH_MS_CONFIG, String.valueOf(9223372036854775807L),
//            INDEX_INTERVAL_BYTES_CONFIG, String.valueOf(4096),
//            MIN_CLEANABLE_DIRTY_RATIO_CONFIG, String.valueOf(0.5),
//            PREALLOCATE_CONFIG, String.valueOf(false),
//            SEGMENT_JITTER_MS_CONFIG, String.valueOf(0),
//            UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, String.valueOf(false)
//    );

    private final Map<String, String> configWithDefaultValue;

    public DefaultConfigValueRule(Map<String, String> configWithDefaultValue) {
        this.configWithDefaultValue = configWithDefaultValue;
    }

    @Override
    public void validate(String topic, Map<String, String> configs) {
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            String key = entry.getKey();
            if (configWithDefaultValue.containsKey(key)) {
                String defaultVal = configWithDefaultValue.get(key);
                String val = entry.getValue();
                if (!val.equals(defaultVal)) {
                    throw new PolicyViolationException(
                            String.format("Topic %s configured with invalid configs: %s:%s. This config only allow default value: %s"
                                    , topic, key, val, defaultVal));
                }
            }
        }
    }
}
