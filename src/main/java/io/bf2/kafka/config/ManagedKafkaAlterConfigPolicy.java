package io.bf2.kafka.config;

import io.bf2.kafka.common.Config;
import io.bf2.kafka.common.rule.ConfigRules;
import io.bf2.kafka.common.PartitionCounter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ManagedKafkaAlterConfigPolicy implements AlterConfigPolicy {
    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaAlterConfigPolicy.class);

    private String privateTopicPrefix;
    private ConfigRules configRules;

    @Override
    public void configure(Map<String, ?> configs) {
        this.configRules = new ConfigRules(configs);

        AbstractConfig parsedConfig = new AbstractConfig(Config.PARTITION_COUNTER_CONFIG_DEF, configs);
        this.privateTopicPrefix = parsedConfig.getString(Config.PRIVATE_TOPIC_PREFIX);
    }

    @Override
    public void close() {}

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        // currently, we only validate "external" TOPIC resources
        if (requestMetadata.resource().type() != ConfigResource.Type.TOPIC ||
                (requestMetadata.resource().type() == ConfigResource.Type.TOPIC && PartitionCounter.isInternalTopic(requestMetadata.resource().name(), privateTopicPrefix))) {
            return;
        }

        validateConfigs(requestMetadata);
    }

    private void validateConfigs(RequestMetadata requestMetadata) throws PolicyViolationException {
        Map<String, String> configs = requestMetadata.configs();
        if (!configs.isEmpty()) {
            configRules.validateTopicConfigs(requestMetadata.resource().name(), configs);
        }
    }
}
