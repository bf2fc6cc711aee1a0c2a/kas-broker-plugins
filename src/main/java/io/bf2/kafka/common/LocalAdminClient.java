package io.bf2.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.SslConfigs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalAdminClient {
    public static final String LISTENER_NAME = Config.CREATE_TOPIC_POLICY_PREFIX + "adminclient-listener.name";
    public static final String LISTENER_PORT = Config.CREATE_TOPIC_POLICY_PREFIX + "adminclient-listener.port";
    public static final String LISTENER_PROTOCOL = Config.CREATE_TOPIC_POLICY_PREFIX + "adminclient-listener.protocol";

    public static Admin create(Map<String, ?> configs) {

        ConfigDef listenerConfigDef = new ConfigDef()
                .define(LISTENER_NAME, Type.STRING, Importance.MEDIUM, "Custom listener name property")
                .define(LISTENER_PORT, Type.INT, Importance.MEDIUM, "Custom listener port property")
                .define(LISTENER_PROTOCOL, Type.STRING, Importance.MEDIUM, "Custom listener protocol property");

        AbstractConfig listenerConfig = new AbstractConfig(listenerConfigDef, configs);

        String bootstrapAddress;
        try {
            bootstrapAddress = String.format("%s:%s",
                    InetAddress.getLocalHost().getCanonicalHostName(), listenerConfig.getInt(LISTENER_PORT));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        Map<String, Object> clientConfig = Stream.concat(
                Stream.of(
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
                        .map(p -> {
                            String configKey =
                                    String.format("listener.name.%s.%s", listenerConfig.getString(LISTENER_NAME), p);
                            String v = getConfigString(configs, configKey);
                            return Map.entry(p, v);
                        })
                        .filter(e -> !"".equals(e.getValue())),
                Stream.of(
                        Map.entry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress),
                        Map.entry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG,
                                listenerConfig.getString(LISTENER_PROTOCOL))))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue));

        return Admin.create(clientConfig);
    }

    private static String getConfigString(Map<String, ?> configs, String key) {
        return Optional.ofNullable(configs.get(key))
                .map(Object::toString)
                .orElse("");
    }
}
