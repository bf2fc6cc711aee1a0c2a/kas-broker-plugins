package io.bf2.kafka.common;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Map;

class LocalAdminClientTest {

    @Test
    void testCreate() throws UnknownHostException {
        Map<String, Object> configs = Map.of(
                "strimzi.authorization.custom-authorizer.adminclient-listener.name", "authorizer",
                "strimzi.authorization.custom-authorizer.adminclient-listener.port", 9097,
                "strimzi.authorization.custom-authorizer.adminclient-listener.protocol", "SSL");
        Admin admin = LocalAdminClient.create(configs);
        admin.close(Duration.ofSeconds(2));
    }

}
