package io.bf2.kafka.authorizer;

import io.bf2.kafka.authorizer.VerifiableAppenderExtension.LoggedEvents;
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
import org.slf4j.event.Level;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VerifiableAppenderExtension.class)
class AuditLoggingControllerTest {

    static Map<String, Object> config;
    private AuditLoggingController auditLoggingController;
    private AuthorizableRequestContext fetchRequestContext;
    private Action infoAction;

    @BeforeAll
    static void initialize() {
        config = ConfigHelper.getConfig(CustomAclAuthorizerTest.class);
    }

    @BeforeEach
    void setUp() {
        auditLoggingController = new AuditLoggingController();
        auditLoggingController.configure(config);

        assertEquals(2, auditLoggingController.aclLoggingMap.size(), "failed to parse Logging config correctly");

        fetchRequestContext = stubRequestContext(ApiKeys.FETCH);
        infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);
    }

    @ParameterizedTest
    @CsvSource({
            "Test log level for unspecified binding is INFO, INFO, IDEMPOTENT_WRITE, OFFSET_COMMIT, CLUSTER, kafka-cluster, User:test",
            "Test log level for specified binding is as expected, DEBUG, DESCRIBE, METADATA, TOPIC, myopictopic, User:test",
            "Test can specify fetch API, TRACE, CLUSTER_ACTION, FETCH, CLUSTER, kafka-cluster, User:test",
            "Test can turn off some operations per topic, TRACE, DESCRIBE, METADATA, TOPIC, __strimzi_canary, User:canary-something",
            "Test prioritization considering level, DEBUG, CLUSTER_ACTION, ALTER_ISR, CLUSTER, kafka-cluster, User:test",
    })
    void testGetLogLevel(String title,
                         Level expLevel,
                         String operation,
                         ApiKeys api,
                         ResourceType resourceType,
                         String resourceName,
                         String principalName) {
        Action action = new Action(AclOperation.fromString(operation),
                new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), 0, true, true);
        when(fetchRequestContext.requestType()).thenReturn((int) api.id);
        when(fetchRequestContext.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principalName));

        assertEquals(expLevel, auditLoggingController.logLevelFor(fetchRequestContext, action), title);
    }

    @Test
    void shouldWindowMessage(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //When
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size());
    }

    @Test
    void shouldOnlyWindowConfiguredMessages(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final ApiKeys[] apiKeys = ApiKeys.values();
        final int minLoggedEventCount = (apiKeys.length * 2) - 2; // By default, only produce and fetch operations are windowed

        //When
        for (ApiKeys value : apiKeys) {
            auditLoggingController.logAuditMessage(stubRequestContext(value), infoAction, true);
            auditLoggingController.logAuditMessage(stubRequestContext(value), infoAction, true);
        }

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size());
    }

    @Test
    void shouldLogEventsAfterWindowExpiry(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        auditLoggingController.configure(config);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        // We expect one log entry as part of the initial logging event
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something extra  logged before window expiry");

        //When
        auditLoggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(minLoggedEventCount < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0 suppressed log event original at .*", Level.INFO);
    }

    @Test
    void shouldIncludeCountOfSuppressedEvents(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        auditLoggingController.configure(config);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        // We expect one log entry as part of the initial logging event
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something extra  logged before window expiry");

        //When
        auditLoggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(minLoggedEventCount < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0 with 2 identical entries suppressed between .* and .*", Level.INFO);
    }

    @Test
    void shouldNotLogSingleEventAfterWindowExpiry(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        auditLoggingController.configure(config);

        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        // We expect one log entry as part of the initial logging event
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something extra  logged before window expiry");

        //When
        auditLoggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something extra logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldIncludeSuppressedCountLogEventsAfterWindowExpiry(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        for (int i = 0; i < 10; i++) {
            auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        }
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged before window expiry");

        //When
        auditLoggingController.evictWindowedEvents(); //Rather than wait for window expiry purge the cache manually

        //Then
        assertTrue(minLoggedEventCount < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0 with 9 identical entries suppressed between .* and .*", Level.INFO);
    }

    @Test
    void shouldNotSuppressLogEventsWithDifferentDecisions(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 2;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //When
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, false);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Denied Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldNotSuppressLogEventsFromDifferentPrincipals(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);
        final AuthorizableRequestContext rc = stubRequestContext(ApiKeys.FETCH);
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2"));

        final int minLoggedEventCount = loggingEvents.size() + 2;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //When
        auditLoggingController.logAuditMessage(rc, infoAction, true);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:user2 is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldNotSuppressLogEventsFromDifferentListeners(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        final AuthorizableRequestContext rc = stubRequestContext(ApiKeys.FETCH);
        when(rc.listenerName()).thenReturn("SRE-9096://0.0.0.0:9096");

        final int minLoggedEventCount = loggingEvents.size() + 2;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //When
        auditLoggingController.logAuditMessage(rc, infoAction, true);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener SRE-9096://0.0.0.0:9096 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldNotSuppressLogEventsFromDifferentResources(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action bazAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);
        Action barAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "__strimizi_canary", PatternType.LITERAL), 0, true, true);


        final int minLoggedEventCount = loggingEvents.size() + 2;
        auditLoggingController.logAuditMessage(fetchRequestContext, bazAction, true);

        //When
        auditLoggingController.logAuditMessage(fetchRequestContext, barAction, true);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:__strimizi_canary for request = FETCH with resourceRefCount = 0", Level.INFO);
    }

    @Test
    void shouldNotSuppressLogEventsFromDifferentOperations(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        final int minLoggedEventCount = loggingEvents.size() + 2;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);

        //When
        auditLoggingController.logAuditMessage(stubRequestContext(ApiKeys.PRODUCE), infoAction, true);

        //Then
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = PRODUCE with resourceRefCount = 0", Level.INFO);
    }


    @Test
    void shouldLogSuppressedEventsOnClose(@LoggedEvents List<LoggingEvent> loggingEvents) {
        //Given
        Action infoAction = new Action(AclOperation.READ,
                new ResourcePattern(ResourceType.TOPIC, "baz", PatternType.LITERAL), 0, true, true);

        final int minLoggedEventCount = loggingEvents.size() + 1;
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        auditLoggingController.logAuditMessage(fetchRequestContext, infoAction, true);
        // We expect one log entry as part of the initial logging event
        assertEquals(minLoggedEventCount, loggingEvents.size(), "Something extra logged before window expiry");

        //When
        auditLoggingController.close();

        //Then
        assertTrue(minLoggedEventCount < loggingEvents.size(), "Nothing logged after window expiry");
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0", Level.INFO);
        assertMessageLogged(loggingEvents, "Principal = User:test is Allowed Operation = Read from host = 127.0.0.1 via listener security-9095 on resource = Topic:LITERAL:baz for request = FETCH with resourceRefCount = 0 suppressed log event original at .*", Level.INFO);
    }

    @Test
    void shouldAllowShutdownBeforeConfigured() {
        //Given
        final AuditLoggingController freshController = new AuditLoggingController();

        //When
        assertDoesNotThrow(freshController::close);

        //Then
    }

    @SuppressWarnings("SameParameterValue")
    private void assertMessageLogged(List<LoggingEvent> loggingEvents, String expectedMessagePattern, Level expectedLevel) {
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
        final Pattern pattern = Pattern.compile(expectedMessagePattern);
        assertTrue(loggingEvents.stream()
                        .filter(loggingEvent -> loggingEvent.getLevel() == log4jLevel)
                        .anyMatch(loggingEvent -> pattern.matcher(loggingEvent.getMessage().toString()).matches()),
                "expected message not logged at " + expectedLevel
        );
    }

    private AuthorizableRequestContext stubRequestContext(ApiKeys apiKeys) {
        final AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn("security-9095");
        when(rc.requestType()).thenReturn((int) apiKeys.id);
        when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test"));
        return rc;
    }
}
