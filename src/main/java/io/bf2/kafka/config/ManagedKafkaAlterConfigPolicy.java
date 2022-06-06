/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.config;

import io.bf2.kafka.common.ConfigRules;
import io.bf2.kafka.common.PartitionCounter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.bf2.kafka.common.PartitionCounter.PRIVATE_TOPIC_PREFIX;
import static io.bf2.kafka.common.PartitionCounter.configDef;

public class ManagedKafkaAlterConfigPolicy implements AlterConfigPolicy {
    private static final Logger log = LoggerFactory.getLogger(ManagedKafkaAlterConfigPolicy.class);

    private Map<String, ?> configs;
    private String privateTopicPrefix;
    private ConfigRules configRules;

    @Override
    public void configure(Map<String, ?> configs) {
        this.configs = configs;
        this.configRules = new ConfigRules(configs);

        AbstractConfig parsedConfig = new AbstractConfig(configDef, this.configs);
        this.privateTopicPrefix = parsedConfig.getString(PRIVATE_TOPIC_PREFIX);
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
