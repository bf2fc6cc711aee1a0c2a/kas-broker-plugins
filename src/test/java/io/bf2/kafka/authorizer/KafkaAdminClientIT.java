package io.bf2.kafka.authorizer;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import org.apache.curator.test.InstanceSpec;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaAdminClientIT {

    AclBindingFilter filterAny = new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, "*", PatternType.ANY),
            new AccessControlEntryFilter("User:*", "*", AclOperation.ANY, AclPermissionType.ANY));

    static EphemeralKafkaBroker broker;
    static int kafkaPlainPort;
    static int kafkaSaslPort;

    @BeforeAll
    static void initialize() throws IOException {
        kafkaPlainPort = InstanceSpec.getRandomPort();
        kafkaSaslPort = InstanceSpec.getRandomPort();

        Properties properties = new Properties();
        try (InputStream stream = KafkaAdminClientIT.class.getResourceAsStream("/kafka-it.properties")) {
            properties.load(stream);
        }
        properties.entrySet().forEach(entry -> {
            entry.setValue(entry.getValue().toString()
                    .replaceAll("\\$\\{kafkaPlainPort\\}", String.valueOf(kafkaPlainPort))
                    .replaceAll("\\$\\{kafkaSaslPort\\}", String.valueOf(kafkaSaslPort)));
        });

        broker = EphemeralKafkaBroker.create(kafkaPlainPort, -1, properties);

        try {
            broker.start().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("Unable to start broker within time limit", e);
        }
    }

    Admin getAdmin(/* KafkaHelper kafkaHelper, */ String username, String password) {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + kafkaSaslPort);
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("org.apache.kafka.common.security.plain.PlainLoginModule "
                + "required "
                + "username=\"%s\" "
                + "password=\"%s\";", username, password));
        return AdminClient.create(config);
    }

    @Test
    void testDescribeAclsUser1NotAuthorized(/* KafkaHelper kafkaHelper */) throws Exception {
        Admin admin = getAdmin("user1", "user1-secret");
        var result = admin.describeAcls(filterAny);
        var bindingFuture = result.values();
        Exception thrown = assertThrows(Exception.class, () -> bindingFuture.get());
        assertEquals(ClusterAuthorizationException.class, thrown.getCause().getClass());
    }

    @Test
    void testDescribeAclsUser2Authorized(/* KafkaHelper kafkaHelper */) throws Exception {
        Admin admin = getAdmin("user2", "user2-secret");
        var result = admin.describeAcls(filterAny);
        var bindingFuture = result.values();
        var bindings = bindingFuture.get();
        assertNotNull(bindings);
    }

}
