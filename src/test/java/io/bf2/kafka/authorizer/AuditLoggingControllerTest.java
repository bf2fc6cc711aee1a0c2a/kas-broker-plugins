package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.slf4j.event.Level;

import java.net.InetAddress;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AuditLoggingControllerTest {

    static Map<String, Object> config;
    private AuditLoggingController auditLoggingController;

    @BeforeAll
    static void initialize() {
        config = ConfigHelper.getConfig(CustomAclAuthorizerTest.class);
    }

    @BeforeEach
    void setUp() {
        auditLoggingController = new AuditLoggingController();
        auditLoggingController.configure(config);

        assertEquals(2, auditLoggingController.aclLoggingMap.size(), "failed to parse Logging config correctly");
    }

    @ParameterizedTest
    @CsvSource({
            "Test log level for unspecified binding is INFO, INFO, IDEMPOTENT_WRITE, OFFSET_COMMIT, CLUSTER, kafka-cluster, User:test",
            "Test log level for specified binding is as expected, DEBUG, DESCRIBE, METADATA, TOPIC, myopictopic, User:test",
            "Test can specify fetch API, TRACE, CLUSTER_ACTION, FETCH, CLUSTER, kafka-cluster, User:test",
            "Test can turn off some operations per topic, TRACE, DESCRIBE, METADATA, TOPIC, __strimzi_canary, User:canary-something",
            "Test prioritization considering level, DEBUG, CLISTER_ACTION, ALTER_ISR, CLUSTER, kafka-cluster, User:test",
    })
    void testGetLogLevel(String title,
                         Level expLevel,
                         String operation,
                         ApiKeys api,
                         ResourceType resourceType,
                         String resourceName,
                         String principalName) {
        AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
        Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        Mockito.when(rc.listenerName()).thenReturn("security-9095");
        Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principalName));
        Mockito.when(rc.requestType()).thenReturn((int) api.id);

        Action action = new Action(AclOperation.fromString(operation),
                new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), 0, true, true);


        assertEquals(expLevel, auditLoggingController.logLevelFor(rc, action), title);
    }

}
