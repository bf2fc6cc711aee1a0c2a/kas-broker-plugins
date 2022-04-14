/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.common;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class PartitionLimitEnforcement {

    public static final String CONFIG_ENABLED = "strimzi.authorization.custom-authorizer.partition-limit-enabled";

    private static final boolean DEFAULT = true;

    private static final ConfigDef configDef = new ConfigDef().define(CONFIG_ENABLED,
            ConfigDef.Type.BOOLEAN, DEFAULT, ConfigDef.Importance.MEDIUM,
            "Feature flag broker property key to allow disabling of partition limit enforcement");

    /**
     * @param config the map of Kafka broker properties.
     * @return false if property explicitly set to false, else true
     */
    public static boolean isEnabled(Map<String, ?> config) {
        AbstractConfig parsedConfig = new AbstractConfig(configDef, config);
        return parsedConfig.getBoolean(CONFIG_ENABLED);
    }
}
