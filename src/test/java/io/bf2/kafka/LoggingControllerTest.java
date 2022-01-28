package io.bf2.kafka;

import io.bf2.kafka.authorizer.AclLoggingConfig;
import io.bf2.kafka.authorizer.ConfigHelper;
import io.bf2.kafka.authorizer.CustomAclAuthorizerTest;
import io.bf2.kafka.authorizer.VerifiableAppenderExtension;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VerifiableAppenderExtension.class)
class LoggingControllerTest {

    private static Map<ResourceType, List<AclLoggingConfig>> loggingConfig;
    private static final Logger targetLogger = LoggerFactory.getLogger(LoggingControllerTest.class);
    private LoggingController loggingController;

    @BeforeAll
    static void initialize() {
        //TODO stop using the prod code to configure the tests
        loggingConfig = LoggingController.extractLoggingConfiguration(ConfigHelper.getConfig(CustomAclAuthorizerTest.class));
    }

    @BeforeEach
    void setup() {
        loggingController = new LoggingController(targetLogger, loggingConfig);
        assertEquals(2, loggingConfig.size(), "unexpected logging config loaded");
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


        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principalName));
        when(rc.requestType()).thenReturn((int) api.id);

        Action action = new Action(AclOperation.fromString(operation),
                new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), 0, true, true);


        assertEquals(expLevel, loggingController.logLevelFor(rc, action), title);
    }


    @Test
    void shouldWindowMessage(@VerifiableAppenderExtension.LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given

        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "User: test"));
        when(rc.requestType()).thenReturn((int) ApiKeys.FETCH.id);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        int alreadyLogged = loggingEvents.size();

        //When
        loggingController.logAuditMessage(rc, infoAction, true);

        //Then
        assertEquals(alreadyLogged, loggingEvents.size());
    }

    @Test
    void shouldLogEventsAfterWindowExpiry(@VerifiableAppenderExtension.LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given

        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test"));
        when(rc.requestType()).thenReturn((int) ApiKeys.FETCH.id);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        int alreadyLogged = loggingEvents.size();
        loggingController.logAuditMessage(rc, infoAction, true);
        assertEquals(alreadyLogged, loggingEvents.size(), "Something logged before window expiry");

        //When
        loggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(alreadyLogged < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldIncludeSuppressedCountLogEventsAfterWindowExpiry(@VerifiableAppenderExtension.LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given

        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test"));
        when(rc.requestType()).thenReturn((int) ApiKeys.FETCH.id);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        int alreadyLogged = loggingEvents.size();
        for (int i = 0; i < 10; i++) {
            loggingController.logAuditMessage(rc, infoAction, true);
        }
        assertEquals(alreadyLogged, loggingEvents.size(), "Something logged before window expiry");

        //When
        loggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(alreadyLogged < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0 with 10 identical entries suppressed", Level.INFO);
        //Arguably this is a fencepost type error, as there were only 9 entries "suppressed" because this one was logged
    }

    @Test
    void shouldNotMergeLogEventsWithDifferentDecisions(@VerifiableAppenderExtension.LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given

        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test"));
        when(rc.requestType()).thenReturn((int) ApiKeys.FETCH.id);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        int alreadyLogged = loggingEvents.size();
        loggingController.logAuditMessage(rc, infoAction, true);
        loggingController.logAuditMessage(rc, infoAction, false);
        assertEquals(alreadyLogged, loggingEvents.size(), "Something logged before window expiry");

        //When
        loggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(alreadyLogged < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Denied Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);

    }

    @Test
    void shouldNotMoveRepeatedMessagesToTraceIfAuthDecisionsDontMatch() {
        //Given

        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "User: test"));
        when(rc.requestType()).thenReturn((int) ApiKeys.FETCH.id);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        assertEquals(Level.INFO, loggingController.logLevelFor(rc, infoAction));

        //When
        Action traceAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        //Then
        assertEquals(Level.INFO, loggingController.logLevelFor(rc, traceAction));
    }

    private void assertMessageLogged(List<LoggingEvent> loggingEvents, String expectedMessage, Level expectedLevel) {
        org.apache.log4j.Level log4jLevel;
        switch (expectedLevel) {
            case ERROR:
                log4jLevel = org.apache.log4j.Level.ERROR;
                break;
            case WARN:
                log4jLevel = org.apache.log4j.Level.WARN;
                break;
            case INFO:
                log4jLevel = org.apache.log4j.Level.INFO;
                break;
            case DEBUG:
                log4jLevel = org.apache.log4j.Level.DEBUG;
                break;
            case TRACE:
                log4jLevel = org.apache.log4j.Level.TRACE;
                break;
            default:
                throw new IllegalArgumentException("Unsupported logging level");
        }
        assertTrue(loggingEvents.stream()
                        .filter(loggingEvent -> loggingEvent.getLevel() == log4jLevel)
                        .anyMatch(loggingEvent -> expectedMessage.equals(loggingEvent.getMessage())),
                "expected message not logged at " + expectedLevel
        );
    }
}
