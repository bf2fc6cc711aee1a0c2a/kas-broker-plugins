package io.bf2.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalAdminClient {

    public static Admin create(Map<String, ?> configs) throws UnknownHostException {
        String listenerName =
                getConfigString(configs, "strimzi.authorization.custom-authorizer.adminclient-listener.name");
        String listenerPort =
                getConfigString(configs, "strimzi.authorization.custom-authorizer.adminclient-listener.port");
        String listenerSecurityProtocol =
                getConfigString(configs, "strimzi.authorization.custom-authorizer.adminclient-listener.protocol");

        String bootstrapAddress = String.format("%s:%s", InetAddress.getLocalHost().getCanonicalHostName(), listenerPort);

        Map<String, Object> clientConfig = Stream.concat(
                Stream.of(
                        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
                        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
                        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)
                        .map(p -> {
                            String configKey = String.format("listener.name.%s.%s", listenerName, p);
                            String v = getConfigString(configs, configKey);
                            return Map.entry(p, v);
                        })
                        .filter(e -> !"".equals(e.getValue())),
                Stream.of(
                        Map.entry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress),
                        Map.entry(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, listenerSecurityProtocol)))
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
