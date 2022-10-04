package io.bf2.kafka.authorizer;

import io.bf2.kafka.common.Config;
import io.bf2.kafka.common.PartitionCounter;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CustomAclAuthorizerTest {

    static Map<String, Object> config;
    kafka.security.authorizer.AclAuthorizer delegate;

    @BeforeAll
    static void initialize() throws IOException {
        config = ConfigHelper.getConfig(CustomAclAuthorizerTest.class);
    }

    @BeforeEach
    void setup() {
        this.delegate = mock(kafka.security.authorizer.AclAuthorizer.class);

        when(this.delegate.authorize(any(AuthorizableRequestContext.class), anyList()))
            .thenAnswer(invocation -> {
                int count = invocation.getArgument(1, List.class).size();
                List<AuthorizationResult> results = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    results.add(AuthorizationResult.DENIED);
                }
                return results;
            });

        when(this.delegate.acls(any(AclBindingFilter.class)))
            .thenReturn(Collections.emptyList());
    }

    @Test
    void testConfigureTallyLoaded() throws IOException {
        final int expected = 13;
        /*
         * Verifies that all records loaded and that there are no equals/hashCode collisions.
         */
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate)) {
            auth.configure(config);

            Set<CustomAclBinding> uniqueBindings = auth.aclMap.values()
                .stream()
                .flatMap(List::stream)
                .collect(Collectors.toCollection(HashSet::new));

            assertEquals(expected, uniqueBindings.size());

            for (CustomAclBinding binding : uniqueBindings) {
                assertEquals(expected - 1, uniqueBindings.stream().filter(b -> !binding.equals(b)).count());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
        "Test ALLOW read of topic foo configured, ALLOW, TOPIC, foo, READ, anylistener, anyprincipal",
        "Test ALLOW write of topic foo configured, ALLOW, TOPIC, foo, WRITE, anylistener, anyprincipal",
        "Test ALLOW create of topic foo configured, ALLOW, TOPIC, foo, CREATE, anylistener, anyprincipal",
        "Test ALLOW read of topic bar configured, ALLOW, TOPIC, bar, READ, anylistener, anyprincipal",
        "Test DENY read of group xyz configured, DENY, GROUP, xyz, READ, anylistener, anyprincipal",
        "Test DENY create of group xyz configured, DENY, GROUP, xyz, CREATE, anylistener, anyprincipal",
        "Test DENY any cluster op from external listener, DENY, CLUSTER, *, ALL, external-9094, anyprincipal"
    })
    void testConfigureBindings(String title,
            AclPermissionType expPermission,
            ResourceType expResourceType,
            String expResourceName,
            AclOperation expOperation,
            String expListener,
            String expPrincipalName) throws IOException {

        KafkaPrincipal expPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, expPrincipalName);

        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate)) {
            auth.configure(config);

            List<CustomAclBinding> matchedBindings = auth.aclMap.get(expResourceType)
                    .stream()
                    .filter(binding -> binding.pattern().resourceType() == expResourceType)
                    .filter(binding -> binding.pattern().name().equals(expResourceName))
                    .filter(binding -> binding.matchesOperation(expOperation))
                    .filter(binding -> binding.entry().permissionType().equals(expPermission))
                    .filter(binding -> binding.matchesListener(expListener))
                    .filter(binding -> binding.matchesPrincipal(expPrincipal))
                    .collect(Collectors.toList());

            assertEquals(1, matchedBindings.size());
            assertFalse(matchedBindings.get(0).isPrincipalSpecified());
        }
    }

    @SuppressWarnings("removal")
    @Test
    void testConfigureAllowedListeners() throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate)) {
            auth.configure(config);

            assertEquals(2, auth.allowedListeners.size());
            assertTrue(auth.allowedListeners.contains("canary"));
            assertTrue(auth.allowedListeners.contains("loop"));
        }
    }

    @ParameterizedTest
    @CsvSource({
        "Test user `any` ALLOWED READ TOPIC `foo` on external listener,       User:any,   external-9024://127.0.0.1:9024, READ,     TOPIC,   foo,  0, ALLOWED",
        "Test user `any` DENIED DELETE TOPIC `foo` on external listener,      User:any,   external-9024://127.0.0.1:9024, DELETE,   TOPIC,   foo,  0, DENIED",
        "Test user `any` ALLOWED READ TOPIC `xyz` on external listener,       User:any,   external-9024://127.0.0.1:9024, READ,     TOPIC,   xyz,  0, ALLOWED",
        "Test user `any` DENIED WRITE TOPIC `xyz` on external listener,       User:any,   external-9024://127.0.0.1:9024, WRITE,    TOPIC,   xyz,  0, DENIED",
        "Test user `any` ALLOWED READ TOPIC `abc1` on external listener,      User:any,   external-9024://127.0.0.1:9024, READ,     TOPIC,  abc1,  0, ALLOWED",
        "Test user `any` ALLOWED WRITE TOPIC `abc2` on external listener,     User:any,   external-9024://127.0.0.1:9024, WRITE,    TOPIC,  abc2,  0, ALLOWED",
        "Test user `any` DENIED READ GROUP `xyz` on external listener,        User:any,   external-9024://127.0.0.1:9024, READ,     GROUP,   xyz,  0, DENIED",
        "Test user `any` DENIED READ CLUSTER `xyz` on external listener,      User:any,   external-9024://127.0.0.1:9024, READ,     CLUSTER, xyz,  0, DENIED",
        "Test user `alice` ALLOWED READ CLUSTER `xyz` on external listener,   User:alice, external-9024://127.0.0.1:9024, READ,     CLUSTER, xyz,  0, ALLOWED",
        "Test user `admin` ALLOWED READ GROUP `xyz` on external listener,     User:admin, external-9024://127.0.0.1:9024, READ,     GROUP,   xyz,  0, ALLOWED",
        "Test user `any` ALLOWED READ GROUP `abc` on loop listener,           User:any,   loop,                           READ,     GROUP,   abc,  0, ALLOWED",
        "Test user `any` ALLOWED READ GROUP `abc` on full loop listener,      User:any,   loop-9021://127.0.0.1:9021,     READ,     GROUP,   abc,  0, ALLOWED",
        "Test user `any` DENIED READ GROUP `abc` on something listener,       User:any,   something,                      READ,     GROUP,   abc,  0, DENIED",
        "Test user `bob` ALLOWED DESCRIBE TOPIC `foo` on external listener,   User:bob,   external-9024://127.0.0.1:9024, DESCRIBE, TOPIC,   foo,  0, ALLOWED",
        "Test user `bob` DENIED READ TOPIC `foo` on external listener,        User:bob,   external-9024://127.0.0.1:9024, READ,     TOPIC,   foo,  0, DENIED",
        // 23: OffsetForLeaderEpoch
        "Test user `alice` DENIED DESCRIBE(23) TOPIC `baa` on external listener, User:alice, external-9024://127.0.0.1:9024, DESCRIBE, TOPIC,   baa, 23, DENIED",
        // 2: ListOffsets
        "Test user `alice` ALLOWED DESCRIBE(2) TOPIC `baa` on external listener, User:alice, external-9024://127.0.0.1:9024, DESCRIBE, TOPIC,   baa,  2, ALLOWED",
        // 3: Metadata
        "Test user `alice` ALLOWED DESCRIBE(3) TOPIC `baa` on external listener, User:alice, external-9024://127.0.0.1:9024, DESCRIBE, TOPIC,   baa,  3, ALLOWED",
        // 9: OffsetFetch
        "Test user `alice` ALLOWED DESCRIBE(9) TOPIC `baa` on external listener, User:alice, external-9024://127.0.0.1:9024, DESCRIBE, TOPIC,   baa,  9, ALLOWED",
    })
    void testAuthorize(String title,
            String principal,
            String listener,
            AclOperation operation,
            ResourceType resourceType,
            String resourceName,
            int requestType,
            AuthorizationResult expectedResult) throws IOException {

        KafkaPrincipal superUser = SecurityUtils.parseKafkaPrincipal("User:admin");
        when(this.delegate.isSuperUser(superUser)).thenReturn(Boolean.TRUE);

        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate)) {
            auth.configure(config);

            String[] principalComponents = principal.split(":");
            AuthorizableRequestContext rc = createMockRequestContext(listener, principalComponents[0], principalComponents[1], requestType);

            Action action = new Action(operation, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), 0, true, true);
            List<AuthorizationResult> results = auth.authorize(rc, Arrays.asList(action));

            assertEquals(1, results.size());
            assertEquals(expectedResult, results.get(0));
        }
    }


    @ParameterizedTest
    @CsvSource({
        "Denied for principal missing 'User:' prefix, user2,ACL rules including principal 'user2' are prohibited - principal is not type User",
        "Denied for principal in static configuration, User:anonymous, ACL rules including principal 'User:anonymous' are prohibited - this principal is restricted"
    })
    void testCreateAclsDeniedForInvalidPrincipal(String title, String principal, String error) throws IOException {
        AclBinding binding1 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                new AccessControlEntry(principal, "*", AclOperation.READ, AclPermissionType.ALLOW));
        AclBinding binding2 = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                new AccessControlEntry(principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW));
        List<AclBinding> bindings = List.of(binding1, binding2);
        createAclsAndExpect("owner1", bindings, (aclCreateResult) -> isFailedWithError(aclCreateResult, error), 2);
    }

    @Test
    void testCreateAclsAllowedWhenPrincipalGivesThemselvesACLRules() throws IOException {
        CompletableFuture<AclCreateResult> successFuture = completedFuture(new AclCreateResult(null));
        doReturn(List.of(successFuture)).when(delegate).createAcls(any(), anyList());

        AclBinding binding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "user1_", PatternType.PREFIXED),
                new AccessControlEntry("User:" + "tom", "*", AclOperation.READ, AclPermissionType.ALLOW));
        createAclsAndExpect("tom", List.of(binding), (aclCreateResult) -> aclCreateResult.exception().isEmpty(), 1);
    }

    @Test
    void testCreateAclsDeniedForInvalidBinding() throws IOException {
        AclBinding readUser1Topics = new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "my-cluster", PatternType.LITERAL),
                new AccessControlEntry("User:user1", "*", AclOperation.CLUSTER_ACTION, AclPermissionType.ALLOW));
        AclBinding writeUser1Topics = new AclBinding(
                new ResourcePattern(ResourceType.CLUSTER, "my-cluster", PatternType.LITERAL),
                new AccessControlEntry("User:user1", "*", AclOperation.IDEMPOTENT_WRITE, AclPermissionType.ALLOW));
        AclBinding describleAllDelegationTokens = new AclBinding(
                new ResourcePattern(ResourceType.DELEGATION_TOKEN, "*", PatternType.LITERAL),
                new AccessControlEntry("User:user1", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW));
        var bindings = List.of(readUser1Topics, writeUser1Topics, describleAllDelegationTokens);
        createAclsAndExpect("owner1", bindings, (aclCreateResult) -> isFailedWithError(aclCreateResult, CustomAclAuthorizer.CREATE_ACL_INVALID_BINDING), 3);
    }

    @Test
    void testDeleteAllBindingsForPrincipalsWithStaticRulesFromDelegateOnConfigure() throws Exception {
        String aliceTopicBinding = "permission=allow;topic=baa;operations=describe;principal=alice";
        String bobTopicBinding = "permission=allow;topic=baa;operations=describe;principal=bob";
        Map<String, String> config = Map.of("kas.authorizer.acl.1", aliceTopicBinding, "kas.authorizer.acl.2", bobTopicBinding);
        whenAuthorizerConstructedAndConfigured(config);
        verify(delegate).deleteAcls(any(), eq(List.of(principalFilter("User:alice"), principalFilter("User:bob"))));
    }

    @Test
    void testOrphanedPrincipalsDeletionToleratesZeroManagedPrincipals() throws Exception {
        Map<String, String> emptyMap = Map.of();
        whenAuthorizerConstructedAndConfigured(emptyMap);
        verify(delegate, never()).deleteAcls(any(), anyList());
    }


    @ParameterizedTest
    @MethodSource("orphanDeletionResults")
    void testOrphanedPrincipalDeletionDoesNotImpactAuthorizerOperation(CompletableFuture<AclDeleteResult> orphanDeletionFuture) throws Exception {
        doReturn(List.of(orphanDeletionFuture)).when(delegate).deleteAcls(any(), anyList());
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, false, false);
        String aliceTopicBinding = "permission=allow;topic=baa;operations=describe;principal=alice";
        Map<String, String> config = Map.of("kas.authorizer.acl.1", aliceTopicBinding);
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate, partitionCounter)) {
            auth.configure(config);
            assertUserPrincipalAllowedToDescribeTopic(auth, "alice", "baa");
        }
    }


    private static Stream<CompletableFuture<AclDeleteResult>> orphanDeletionResults() {
        CompletableFuture<AclDeleteResult> deleteResultException = completedFuture(new AclDeleteResult(new ApiException()));
        CompletableFuture<AclDeleteResult> nullAclBindingResults = completedFuture(new AclDeleteResult((Collection<AclDeleteResult.AclBindingDeleteResult>) null));
        return Stream.of(
                failedFuture(new RuntimeException("boom")),
                completedFuture(null),
                deleteResultException,
                nullAclBindingResults,
                deleteBindingResultWithException(),
                successfulBindingResult()
        );
    }

    private static CompletableFuture<AclDeleteResult> deleteBindingResultWithException() {
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "baa", PatternType.LITERAL);
        AccessControlEntry controlEntry = new AccessControlEntry("principal", "host", AclOperation.ALL, AclPermissionType.DENY);
        AclBinding binding = new AclBinding(pattern, controlEntry);
        AclDeleteResult.AclBindingDeleteResult bindingDeleteResult = new AclDeleteResult.AclBindingDeleteResult(binding, new ApiException());
        AclDeleteResult resultWithException = new AclDeleteResult(List.of(bindingDeleteResult));
        return completedFuture(resultWithException);
    }

    private static CompletableFuture<AclDeleteResult> successfulBindingResult() {
        ResourcePattern pattern = new ResourcePattern(ResourceType.TOPIC, "baa", PatternType.LITERAL);
        AccessControlEntry controlEntry = new AccessControlEntry("principal", "host", AclOperation.ALL, AclPermissionType.DENY);
        AclBinding binding = new AclBinding(pattern, controlEntry);
        AclDeleteResult.AclBindingDeleteResult bindingDeleteResult = new AclDeleteResult.AclBindingDeleteResult(binding);
        AclDeleteResult deleteResult = new AclDeleteResult(List.of(bindingDeleteResult));
        return completedFuture(deleteResult);
    }

    @ParameterizedTest
    @CsvSource({
            "null, ALLOWED, false",
            "true, DENIED, false",
            "false, ALLOWED, false",
            "null, ALLOWED, true",
            "true, ALLOWED, true",
            "false, ALLOWED, true"
    })
    void testPartitionLimitEnforcementFeatureFlag(String featureFlag, AuthorizationResult result, boolean isSuperUser)
            throws Exception {
        KafkaPrincipal superUser = SecurityUtils.parseKafkaPrincipal("User:admin");
        when(this.delegate.isSuperUser(superUser)).thenReturn(isSuperUser);
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, false, Boolean.parseBoolean(featureFlag));
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate, partitionCounter)) {
            Map<String, Object> customConfig = new HashMap<>(config);

            if (!"null".equalsIgnoreCase(featureFlag)) {
                customConfig.put(Config.LIMIT_ENFORCED, featureFlag);
            }

            auth.configure(customConfig);

            AuthorizableRequestContext rc = createMockRequestContext("security-9095", KafkaPrincipal.USER_TYPE, "admin", ApiKeys.CREATE_PARTITIONS.id);

            Action action = new Action(AclOperation.ALTER, new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL), 0, true, true);
            List<AuthorizationResult> results = auth.authorize(rc, List.of(action));

            assertEquals(1, results.size());
            assertEquals(result, results.get(0));
        }
    }


    private void whenAuthorizerConstructedAndConfigured(Map<String, String> configuration) throws InterruptedException, ExecutionException, TimeoutException, IOException {
        PartitionCounter partitionCounter = generateMockPartitionCounter(1001, false, false);
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate, partitionCounter)) {
            auth.configure(configuration);
        }
    }

    private void createAclsAndExpect(String user, List<AclBinding> bindings, Predicate<AclCreateResult> allMatch, int expectedResults) throws IOException {
        try (CustomAclAuthorizer auth = new CustomAclAuthorizer(this.delegate)) {
            auth.configure(config);

            AuthorizableRequestContext rc = createMockRequestContext("security-9095", KafkaPrincipal.USER_TYPE, user, ApiKeys.CREATE_ACLS.id);

            var results = auth.createAcls(rc, bindings);

            List<AclCreateResult> createResults = results.stream()
                    .map(CompletionStage::toCompletableFuture)
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());

            assertEquals(expectedResults, createResults.size());
            assertTrue(createResults.stream().allMatch(allMatch));
        }
    }

    private static AuthorizableRequestContext createMockRequestContext(String listener, String principalType, String principalName, int requestType) {
        AuthorizableRequestContext rc = mock(AuthorizableRequestContext.class);
        when(rc.clientAddress()).thenReturn(InetAddress.getLoopbackAddress());
        when(rc.listenerName()).thenReturn(listener);
        when(rc.principal()).thenReturn(new KafkaPrincipal(principalType, principalName));
        when(rc.requestType()).thenReturn(requestType);
        return rc;
    }

    private static boolean isFailedWithError(AclCreateResult aclCreateResult, String error) {
        return aclCreateResult.exception().isPresent() && error.equals(aclCreateResult.exception().get().getMessage());
    }

    private static AclBindingFilter principalFilter(String principal) {
        AccessControlEntryFilter principalFilter = new AccessControlEntryFilter(principal, null, AclOperation.ANY, AclPermissionType.ANY);
        return new AclBindingFilter(ResourcePatternFilter.ANY, principalFilter);
    }

    private static void assertUserPrincipalAllowedToDescribeTopic(CustomAclAuthorizer auth, String principal, String topic) {
        AuthorizableRequestContext rc = createMockRequestContext("security-9095", KafkaPrincipal.USER_TYPE, principal, ApiKeys.DESCRIBE_CONFIGS.id);
        Action action = new Action(AclOperation.DESCRIBE, new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), 0, true, true);
        List<AuthorizationResult> results = auth.authorize(rc, List.of(action));
        assertEquals(1, results.size());
        assertEquals(AuthorizationResult.ALLOWED, results.get(0));
    }

    private PartitionCounter generateMockPartitionCounter(int numPartitions, boolean response, boolean limitEnforced)
            throws InterruptedException, ExecutionException, TimeoutException {
        PartitionCounter partitionCounter = mock(PartitionCounter.class);
        when(partitionCounter.getMaxPartitions()).thenReturn(1000);
        when(partitionCounter.getExistingPartitionCount()).thenReturn(numPartitions);
        when(partitionCounter.countExistingPartitions()).thenReturn(numPartitions);
        when(partitionCounter.reservePartitions(anyInt())).thenReturn(response);
        when(partitionCounter.isLimitEnforced()).thenReturn(limitEnforced);

        return partitionCounter;
    }

}
