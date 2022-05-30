package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.bf2.kafka.authorizer.CustomAclAuthorizer.ACL_PREFIX;
import static io.bf2.kafka.authorizer.CustomAclAuthorizer.ALLOWED_LISTENERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomAclAuthorizerIT {

    static class ZooKeeperServer extends ZooKeeperServerMain {
        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    static ZooKeeperServer zk;
    static Thread zkThread;
    static int zkPort;

    @BeforeAll
    static void initialize() throws IOException {
        System.setProperty("zookeeper.admin.enableServer", "false");
        zk = new ZooKeeperServer();
        final ServerConfig configuration = new ServerConfig();

        try (ServerSocket allocation = new ServerSocket(0)) {
            zkPort = allocation.getLocalPort();
        }

        configuration.parse(new String[] {
                String.valueOf(zkPort),
                Files.createTempDirectory("group-authorizer").toString()
        });

        zkThread = new Thread() {
            @Override
            public void run() {
                try {
                    zk.runFromConfig(configuration);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } catch (AdminServerException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        zkThread.start();
    }

    @AfterAll
    static void shutdown() throws InterruptedException {
        zk.shutdown();
        zkThread.join();
    }

    @AfterEach
    void cleanup() throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            Map<String, Object> config = getConfig();
            auth.configure(config);
            auth.deleteAcls(null, List.of(AclBindingFilter.ANY));
        }
    }

    @SuppressWarnings("removal")
    static Map<String, Object> getConfig(String... testAcls) {
        Map<String, Object> config = new HashMap<>();
        config.put("super.users", "User:admin;User:owner-123");
        config.put("zookeeper.connect", "127.0.0.1:" + zkPort);
        // Always reject cluster operations from external listener
        config.put(ACL_PREFIX + "1", "permission=deny;listeners=external.*;cluster=*;operations=all");
        for (int i = 0; i < testAcls.length; i++) {
            config.put(ACL_PREFIX + String.valueOf(i + 2), testAcls[i]);
        }
        config.put(ALLOWED_LISTENERS, "PLAIN-9092,SRE-9096");
        config.put("kas.policy.shared-admin.adminclient-listener.name", "PLAIN-9092");
        config.put("kas.policy.shared-admin.adminclient-listener.port", "9092");
        config.put("kas.policy.shared-admin.adminclient-listener.protocol", "PLAINTEXT");
        return config;
    }

    static void createAclsUser1(CustomAclAuthorizer auth) {
        AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
        Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        Mockito.when(rc.listenerName()).thenReturn("security-9095");
        Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "owner1"));
        Mockito.when(rc.requestType()).thenReturn((int) ApiKeys.CREATE_ACLS.id);

        KafkaPrincipal user1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user1");

        AclBinding readUser1Topics = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                new AccessControlEntry(user1.toString(), "*", AclOperation.READ, AclPermissionType.ALLOW));
        AclBinding writeUser1Topics = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                new AccessControlEntry(user1.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW));

        auth.createAcls(rc, List.of(readUser1Topics, writeUser1Topics));
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/config-invalid-cases.txt", delimiter = '|', lineSeparator = "@\n", nullValues = "-")
    void testConfigureInvalidCases(String title, String acl, String exceptionMsgPrefix) throws IOException {
        IllegalArgumentException thrown;

        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            Map<String, Object> config = getConfig(acl);
            thrown = assertThrows(IllegalArgumentException.class, () -> auth.configure(config));
        }

        assertTrue(thrown.getMessage().startsWith(exceptionMsgPrefix), () -> "Unexpected message: " + thrown.getMessage());
    }

    @ParameterizedTest
    @CsvFileSource(resources = "/custom-acl-cases.txt", delimiter = '|', lineSeparator = "@\n", nullValues = "-")
    void testAuthorizeCustomAcls(String title,
            String acl,
            String principal,
            AclOperation operation,
            ApiKeys apiKey,
            ResourceType resourceType,
            String resourceName,
            String listener,
            AuthorizationResult expectation) throws IOException {

        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
            Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
            Mockito.when(rc.listenerName()).thenReturn(listener);
            Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal));
            Mockito.when(rc.requestType()).thenReturn((int) apiKey.id);

            auth.configure(getConfig(acl));
            ResourcePattern resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
            List<Action> actions = List.of(new Action(operation, resource, 0, false, false));
            List<AuthorizationResult> actual = auth.authorize(rc, actions);
            assertEquals(List.of(expectation), actual);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "owner1, DESCRIBE, DESCRIBE_ACLS, CLUSTER, my-cluster, external-9094, DENIED",
        "owner1, DESCRIBE, DESCRIBE_ACLS, CLUSTER, my-cluster, secure-9095, ALLOWED",
        // Sampling of healthcheck1 ALLOWED operations
        "healthcheck1, READ,     FETCH,       TOPIC, healthcheck1_private, secure-9095, ALLOWED",
        "healthcheck1, WRITE,    PRODUCE,     TOPIC, healthcheck1_private, secure-9095, ALLOWED",
        "healthcheck1, READ,     JOIN_GROUP,  TOPIC, healthcheck1_private, secure-9095, ALLOWED",
        "healthcheck1, READ,     LEAVE_GROUP, TOPIC, healthcheck1_private, secure-9095, ALLOWED",
        // Sampling of healthcheck1 DENIED operations
        "healthcheck1, READ,     FETCH,           TOPIC, not_healthcheck1_private, secure-9095, DENIED",
        "healthcheck1, DESCRIBE, DESCRIBE_GROUPS, TOPIC, healthcheck1_private,     secure-9095, DENIED",
        // Sampling of readconfig1 ALLOWED operations
        "readconfig1, DESCRIBE_CONFIGS, DESCRIBE_CONFIGS, CLUSTER, my-cluster,     secure-9095, ALLOWED",
        "readconfig1, DESCRIBE,         DESCRIBE_ACLS,    CLUSTER, my-cluster,     secure-9095, ALLOWED",
        "readconfig1, DESCRIBE,         FIND_COORDINATOR, TRANSACTIONAL_ID, txn-1, secure-9095, ALLOWED",
        // Sampling of readconfig1 DENIED operations
        "readconfig1, READ,     FETCH,         TOPIC,   business_data,        secure-9095,   DENIED",
        "readconfig1, WRITE,    PRODUCE,       TOPIC,   healthcheck1_private, secure-9095,   DENIED",
        "readconfig1, DESCRIBE, DESCRIBE_ACLS, CLUSTER, my-cluster,           external-9094, DENIED",
        // Sample of user1 ALLOWED operations
        "user1,       READ,     FETCH,        TOPIC, user1_test_topic,     external-9094, ALLOWED",
        "user1,       WRITE,    PRODUCE,      TOPIC, user1_test_topic,     external-9094, ALLOWED",
        // ACL authorizer treats `describe` as implied from `read`
        "user1,       DESCRIBE, LIST_OFFSETS, TOPIC, user1_test_topic,     external-9094, ALLOWED",
        // Sample of user1 DENIED operations
        "user1,       READ,     FETCH,        TOPIC, user2_test_topic,     external-9094, DENIED",
        // Sample of user1 ALLOWED operations, relies on "default" ACL
        "user1,       READ,     FETCH,        TOPIC, pub,                  external-9094, ALLOWED",
    })
    void testAuthorizeCoreConfigs(String principal,
            AclOperation operation,
            ApiKeys apiKey,
            ResourceType resourceType,
            String resourceName,
            String listener,
            AuthorizationResult expectation) throws IOException {

        Map<String, Object> config = ConfigHelper.getConfig(getClass(), "core", "zookeeper.connect=127.0.0.1:" + zkPort);

        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            auth.configure(config);

            // Seed the system with ACLs for User:user1
            createAclsUser1(auth);

            AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
            Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
            Mockito.when(rc.listenerName()).thenReturn(listener);
            Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal));
            Mockito.when(rc.requestType()).thenReturn((int) apiKey.id);

            ResourcePattern resource = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL);
            List<Action> actions = List.of(new Action(operation, resource, 0, false, false));
            List<AuthorizationResult> actual = auth.authorize(rc, actions);
            assertEquals(List.of(expectation), actual);
        }
    }

    @Test
    void testCreateAclsAreRetrievable() throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            Map<String, Object> config = ConfigHelper.getConfig(CustomAclAuthorizerIT.class, "core", "zookeeper.connect=127.0.0.1:" + zkPort);
            auth.configure(config);

            AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
            Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
            Mockito.when(rc.listenerName()).thenReturn("security-9095");
            Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "owner1"));
            Mockito.when(rc.requestType()).thenReturn((int) ApiKeys.CREATE_ACLS.id);

            KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2");

            AclBinding readUser1Topics = new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntry(user2.toString(), "*", AclOperation.READ, AclPermissionType.ALLOW));
            AclBinding writeUser1Topics = new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntry(user2.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW));

            var createResults = auth.createAcls(rc, List.of(readUser1Topics, writeUser1Topics));
            assertEquals(2, createResults.size());
            assertTrue(createResults.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .map(CompletableFuture::join)
                    .map(AclCreateResult::exception)
                    .noneMatch(Optional::isPresent));

            var listReadAclsIter = auth.acls(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_topicA", PatternType.MATCH),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.READ, AclPermissionType.ANY)));
            var listReadAcls = StreamSupport.stream(listReadAclsIter.spliterator(), false).collect(Collectors.toList());
            assertEquals(1, listReadAcls.size());
            assertEquals(readUser1Topics, listReadAcls.get(0));

            var listWriteAclsIter = auth.acls(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.WRITE, AclPermissionType.ANY)));
            var listWriteAcls = StreamSupport.stream(listWriteAclsIter.spliterator(), false).collect(Collectors.toList());
            assertEquals(1, listWriteAcls.size());
            assertEquals(writeUser1Topics, listWriteAcls.get(0));

            var listAllAclsIter = auth.acls(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.ANY, AclPermissionType.ANY)));
            var listAllAcls = StreamSupport.stream(listAllAclsIter.spliterator(), false).collect(Collectors.toList());
            assertEquals(2, listAllAcls.size());
            assertTrue(listAllAcls.contains(readUser1Topics));
            assertTrue(listAllAcls.contains(writeUser1Topics));
        }
    }

    @Test
    void testCreateAclsAreDeleted() throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            Map<String, Object> config = ConfigHelper.getConfig(CustomAclAuthorizerIT.class, "core", "zookeeper.connect=127.0.0.1:" + zkPort);
            auth.configure(config);

            AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
            Mockito.when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
            Mockito.when(rc.listenerName()).thenReturn("security-9095");
            Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "owner1"));
            Mockito.when(rc.requestType()).thenReturn((int) ApiKeys.CREATE_ACLS.id);

            KafkaPrincipal user2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "user2");

            AclBinding readUser1Topics = new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntry(user2.toString(), "*", AclOperation.READ, AclPermissionType.ALLOW));
            AclBinding writeUser1Topics = new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntry(user2.toString(), "*", AclOperation.WRITE, AclPermissionType.ALLOW));

            var createResults = auth.createAcls(rc, List.of(readUser1Topics, writeUser1Topics));
            assertEquals(2, createResults.size());
            assertTrue(createResults.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .map(CompletableFuture::join)
                    .map(AclCreateResult::exception)
                    .noneMatch(Optional::isPresent));

            var deleteReadAcls = auth.deleteAcls(rc, List.of(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_topicA", PatternType.MATCH),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.READ, AclPermissionType.ANY))));

            assertEquals(1, deleteReadAcls.size());
            assertTrue(deleteReadAcls.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .map(CompletableFuture::join)
                    .map(AclDeleteResult::aclBindingDeleteResults)
                    .flatMap(Collection::stream)
                    .map(AclBindingDeleteResult::aclBinding)
                    .allMatch(readUser1Topics::equals));

            var deleteWriteAcls = auth.deleteAcls(rc, List.of(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.WRITE, AclPermissionType.ANY))));
            assertEquals(1, deleteWriteAcls.size());
            assertTrue(deleteWriteAcls.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .map(CompletableFuture::join)
                    .map(AclDeleteResult::aclBindingDeleteResults)
                    .flatMap(Collection::stream)
                    .map(AclBindingDeleteResult::aclBinding)
                    .allMatch(writeUser1Topics::equals));

            var listAllAclsIter = auth.acls(new AclBindingFilter(
                    new ResourcePatternFilter(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                    new AccessControlEntryFilter(user2.toString(), "*", AclOperation.ANY, AclPermissionType.ANY)));
            var listAllAcls = StreamSupport.stream(listAllAclsIter.spliterator(), false).collect(Collectors.toList());
            assertEquals(0, listAllAcls.size());
        }
    }

    @Test
    void testConfigureDefaultsInvalidIgnored() throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer()) {
            Map<String, Object> config = getConfig();
            auth.configure(config);

            var defaultBindings = new ArrayList<AclBinding>();
            // `apis` dropped by parser
            defaultBindings.addAll(CustomAclBinding.valueOf("default=true;permission=allow;topic=pub;operations=all;apis=fetch"));
            // `listeners` dropped by parser
            defaultBindings.addAll(CustomAclBinding.valueOf("default=true;permission=deny;group=priv;operations=all;listeners=SPECIAL.*"));

            // Invalid binding
            defaultBindings.add(new AclBinding(new ResourcePattern(ResourceType.CLUSTER, "*", PatternType.LITERAL),
                                               new AccessControlEntry("User:*", "*", AclOperation.ALL, AclPermissionType.UNKNOWN)));

            auth.configureDefaults(defaultBindings);
            assertEquals(2, StreamSupport.stream(auth.acls(AclBindingFilter.ANY).spliterator(), false).count());
        }
    }
}
