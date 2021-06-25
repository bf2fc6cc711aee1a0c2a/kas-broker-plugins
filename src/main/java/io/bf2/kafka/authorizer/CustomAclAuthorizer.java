/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A authorizer for Kafka that defines custom ACLs. The configuration is provided as
 * <pre>
 * acl.1: permission=allow;topic=foo;operations=read,write,create
 * acl.2: permission=allow;topic=bar;operations=read
 * acl.3: permission=deny;group=xyz;operations=read,create
 * </pre>
 * supported resource types are "topic, group, cluster, transaction_id, deletegation_token"
 * supported permissions are "allow" and "deny"
 * supported operations are "all,read,write,delete,create,describe,alter,cluster_action,describe_configs,alter_configs"
 */
public class CustomAclAuthorizer extends kafka.security.authorizer.AclAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(CustomAclAuthorizer.class);

    static final String CREATE_ACL_NOT_SUPPORTED = "ACL creation not supported";
    static final String CREATE_ACL_INVALID_PRINCIPAL = "Invalid ACL principal name";
    static final String CREATE_ACL_INVALID_BINDING = "Invalid ACL resource or operation";

    static final String DELETE_ACL_NOT_SUPPORTED = "ACL deletion is not supported";

    static final String CONFIG_PREFIX = "strimzi.authorization.global-authorizer.";

    static final String ALLOWED_ACLS = CONFIG_PREFIX + "allowed-acls";
    /**
     * For backward-compatibility with {@link GlobalAclAuthorizer}.
     *
     * @deprecated switch to a custom authenticated user instead
     */
    @Deprecated(forRemoval = true)
    static final String ALLOWED_LISTENERS = CONFIG_PREFIX + "allowed-listeners";
    static final String ACL_PREFIX = CONFIG_PREFIX + "acl.";
    static final String INTEGRATION_ACTIVE = CONFIG_PREFIX + "integration-active";

    static final Map<AclPermissionType, AuthorizationResult> permissionResults = Map.ofEntries(
            Map.entry(AclPermissionType.ALLOW, AuthorizationResult.ALLOWED),
            Map.entry(AclPermissionType.DENY, AuthorizationResult.DENIED));

    final Map<ResourceType, List<CustomAclBinding>> aclMap = new EnumMap<>(ResourceType.class);
    final Map<String, List<String>> allowedAcls = new HashMap<>();
    final Set<String> aclPrincipals = new HashSet<>();

    /**
     * For backward-compatibility with {@link GlobalAclAuthorizer}.
     *
     * @deprecated switch to a custom authenticated user instead
     */
    @Deprecated(forRemoval = true)
    final Set<String> allowedListeners = new HashSet<>();

    /**
     * Used for testing. Indicates whether integration with ZooKeeper (via the super class) is enabled.
     * Integration may be disabled for unit testing.
     */
    boolean integrationActive;

    @Override
    public void configure(Map<String, ?> configs) {
        Object integrate = Optional.<Object>ofNullable(configs.get(INTEGRATION_ACTIVE)).orElse("true");

        if (Boolean.parseBoolean(integrate.toString())) {
            integrationActive = true;
            super.configure(configs);
        }

        addAllowedListeners(configs);

        if (configs.containsKey(ALLOWED_ACLS)) {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, List<String>>> typeRef = new TypeReference<HashMap<String, List<String>>>() {};

            try {
                allowedAcls.putAll(mapper.readValue(String.valueOf(configs.get(ALLOWED_ACLS)), typeRef));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(ALLOWED_ACLS, e);
            }
        }

        log.info("Allowed Custom Group Authorizer Listeners {}", this.allowedListeners);
        Pattern aclConfig = Pattern.compile(Pattern.quote(ACL_PREFIX) + "\\d+");

        // read custom ACLs configured for rest of the users
        configs.entrySet()
            .stream()
            .filter(config -> aclConfig.matcher(config.getKey()).matches())
            // Order significant for unit test
            .sorted((c1, c2) -> Integer.compare(parseAclSequence(c1), parseAclSequence(c2)))
            .map(Map.Entry::getValue)
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .map(CustomAclBinding::valueOf)
            .flatMap(List::stream)
            .forEach(binding -> {
                aclMap.compute(binding.pattern().resourceType(), (k, v) -> {
                    List<CustomAclBinding> bindings = Objects.requireNonNullElseGet(v, ArrayList::new);
                    bindings.add(binding);
                    return bindings;
                });
                if (binding.isPrincipalSpecified()) {
                    aclPrincipals.add(binding.entry().principal());
                }
            });

        if (log.isInfoEnabled()) {
            log.info("Custom Authorizer ACLs configured:\n\t{}",
                    aclMap.values()
                        .stream()
                        .flatMap(List::stream)
                        .map(Object::toString)
                        .collect(Collectors.joining(",\n\t")));
        }
    }

    /**
     * For backward-compatibility with {@link GlobalAclAuthorizer}.
     *
     * @deprecated switch to a custom authenticated user instead
     */
    @Deprecated(forRemoval = true)
    void addAllowedListeners(Map<String, ?> configs) {
        allowedListeners.clear();
        Object propertyValue = configs.get(ALLOWED_LISTENERS);

        if (propertyValue instanceof String) {
            CustomAclBinding.splitOnComma((String) propertyValue)
                .stream()
                .forEach(listener -> allowedListeners.add(listener.trim()));
        }
    }

    int parseAclSequence(Map.Entry<String, ?> config) {
        String key = config.getKey();
        return Integer.parseInt(key.substring(key.lastIndexOf('.') + 1));
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        return actions.stream()
            .map(action -> authorizeAction(requestContext, action))
            .collect(Collectors.toList());
    }

    private AuthorizationResult authorizeAction(AuthorizableRequestContext requestContext, Action action) {

        // is super user allow any operation
        if (isSuperUser(requestContext.principal())) {
            if (log.isDebugEnabled()) {
                log.debug("super.user {} allowed for operation {} on resource {} using listener {}",
                        requestContext.principal().getName(),
                        action.operation(),
                        action.resourcePattern().name(),
                        requestContext.listenerName());
            }
            return AuthorizationResult.ALLOWED;
        }

        // if request made on any allowed listeners allow always
        if (isAllowedListener(requestContext.listenerName())) {
            if (log.isDebugEnabled()) {
                log.debug("listener {} allowed for operation {} on resource {} using listener {}",
                        requestContext.listenerName(),
                        action.operation(),
                        action.resourcePattern().name(),
                        requestContext.listenerName());
            }
            return AuthorizationResult.ALLOWED;
        }

        // for all other uses lets check the permission
        if (log.isDebugEnabled()) {
            log.debug("User {} asking for permission {} using listener {}",
                    requestContext.principal().getName(),
                    action.operation(),
                    requestContext.listenerName());
        }

        return aclMap.getOrDefault(action.resourcePattern().resourceType(), Collections.emptyList())
                .stream()
                .filter(binding -> binding.matchesResource(action.resourcePattern().name()))
                .filter(binding -> binding.matchesOperation(action.operation()))
                .filter(binding -> binding.matchesApiKey(requestContext.requestType()))
                .filter(binding -> binding.matchesPrincipal(requestContext.principal()))
                .filter(binding -> binding.matchesListener(requestContext.listenerName()))
                .sorted(this::denyFirst)
                .findFirst()
                .map(binding -> resultFromBinding(requestContext, action, binding))
                .orElseGet(() -> delegateOrDeny(requestContext, action));
    }

    int denyFirst(AclBinding b1, AclBinding b2) {
        AclPermissionType p1 = b1.entry().permissionType();
        AclPermissionType p2 = b2.entry().permissionType();

        if (p1 == p2) {
            return 0;
        }

        return p1 == AclPermissionType.DENY ? -1 : 1;
    }

    AuthorizationResult resultFromBinding(AuthorizableRequestContext requestContext, Action action, AclBinding binding) {
        AuthorizationResult result = permissionResults.get(binding.entry().permissionType());
        logAuditMessage(requestContext, action, result == AuthorizationResult.ALLOWED);
        return result;
    }

    /**
     * Deny the request if integration with Kafka ACL handling is disabled or the request principal
     * has custom ACL bindings configured. Otherwise, delegate the authorization request to Kafka
     * ACL handling in the super-class.
     *
     * @param requestContext current request context
     * @param action the action to verify authorization
     * @return the result of the delegated authorization attempt or DENIED
     */
    AuthorizationResult delegateOrDeny(AuthorizableRequestContext requestContext, Action action) {
        if (!integrationActive || hasPrincipalBindings(requestContext.principal().toString())) {
            logAuditMessage(requestContext, action, false);
            return AuthorizationResult.DENIED;
        }
        // Indeterminate result - delegate to default ACL handling
        return super.authorize(requestContext, List.of(action)).get(0);
    }

    boolean hasPrincipalBindings(String principalName) {
        return aclPrincipals.contains(principalName);
    }

    // openshift format PLAIN-9092://0.0.0.0:9092,OPEN-9093://0.0.0.0:9093,SRE-9096://0.0.0.0:9096
    // minikube PLAIN-9092,OPEN-9093,SRE-9096
    public boolean isAllowedListener(String listener) {
        return allowedListeners.stream().anyMatch(listener::startsWith);
    }

    @Override
    public void logAuditMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        if (authorized && action.logIfAllowed()) {
            if (log.isDebugEnabled()) {
                log.debug(buildLogMessage(requestContext, action, authorized));
            }
        } else if (!authorized && action.logIfDenied()) {
            if (log.isInfoEnabled()) {
                log.info(buildLogMessage(requestContext, action, authorized));
            }
        } else if (log.isTraceEnabled()) {
            log.trace(buildLogMessage(requestContext, action, authorized));
        }
    }

    private String buildLogMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        Principal principal = requestContext.principal();
        String operation = SecurityUtils.operationName(action.operation());
        String host = requestContext.clientAddress().getHostAddress();
        String listenerName = requestContext.listenerName();
        String resourceType = SecurityUtils.resourceTypeName(action.resourcePattern().resourceType());
        String authResult = authorized ? "Allowed" : "Denied";
        Object apiKey = ApiKeys.hasId(requestContext.requestType()) ? ApiKeys.forId(requestContext.requestType()).name() : requestContext.requestType();
        int refCount = action.resourceReferenceCount();

        return String.format("Principal = %s is %s Operation = %s from host = %s via listener %s on resource = %s%s%s%s%s for request = %s with resourceRefCount = %s",
                principal,
                authResult,
                operation,
                host,
                listenerName,
                resourceType,
                AclEntry.ResourceSeparator(),
                action.resourcePattern().patternType(),
                AclEntry.ResourceSeparator(),
                action.resourcePattern().name(),
                apiKey,
                refCount);
    }

    boolean isAclBindingAllowed(AclBinding binding) {
        if (allowedAcls.isEmpty()) {
            return true;
        }

        String resourceTypeName = binding.pattern().resourceType().name().toLowerCase();

        if (!allowedAcls.containsKey(resourceTypeName)) {
            return false;
        }

        String operationName = binding.entry().operation().name().toLowerCase();
        return allowedAcls.get(resourceTypeName).contains(operationName);
    }

    @Override
    public List<CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {

        return aclBindings.stream()
            .map(binding -> {
                final CompletionStage<AclCreateResult> result;

                if (!integrationActive) {
                    log.debug("Integration disabled, ACL creation not allowed");
                    result = errorResult(AclCreateResult::new, CREATE_ACL_NOT_SUPPORTED);
                } else if (!binding.entry().principal().startsWith(CustomAclBinding.USER_TYPE_PREFIX)) {
                    /* Reject ACL operations as invalid where the principal named in the ACL binding is the principal performing the operation */
                    log.info("Rejected attempt by user {} to create ACL binding with invalid principal name: {}",
                            requestContext.principal().getName(),
                            binding.entry().principal());
                    result = errorResult(AclCreateResult::new, CREATE_ACL_INVALID_PRINCIPAL);
                } else if (Objects.equals(requestContext.principal().toString(), binding.entry().principal())) {
                    /* Reject ACL operations as invalid where the principal named in the ACL binding is the principal performing the operation */
                    log.info("Rejected attempt by user {} to self-assign ACL binding",
                            requestContext.principal().getName());
                    result = errorResult(AclCreateResult::new, CREATE_ACL_INVALID_PRINCIPAL);
                } else if (hasPrincipalBindings(binding.entry().principal())) {
                    /* Reject ACL operations as invalid where the principal named in the ACL binding is a principal with configured custom ACLs */
                    log.info("Rejected attempt by user {} to create ACL binding for principal {} with existing custom ACL configuration",
                            requestContext.principal().getName(),
                            binding.entry().principal());
                    result = errorResult(AclCreateResult::new, CREATE_ACL_INVALID_PRINCIPAL);
                } else if (!isAclBindingAllowed(binding)) {
                    /* Request to create an ACL that is not explicitly allowed */
                    log.info("Rejected attempt by user {} to create ACL binding for principal {} with existing custom ACL configuration",
                            requestContext.principal().getName(),
                            binding.entry().principal());
                    result = errorResult(AclCreateResult::new, CREATE_ACL_INVALID_BINDING);
                } else {
                    log.debug("Delegating createAcls to parent");
                    result = super.createAcls(requestContext, List.of(binding)).get(0);
                }

                return result;
            })
            .collect(Collectors.toList());
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {

        if (!integrationActive) {
            log.debug("Integration disabled, ACL deletion not allowed");

            return aclBindingFilters.stream()
                    .map(filter -> errorResult(AclDeleteResult::new, DELETE_ACL_NOT_SUPPORTED))
                    .collect(Collectors.toList());
        }

        return super.deleteAcls(requestContext, aclBindingFilters);
    }

    <T> CompletionStage<T> errorResult(Function<ApiException, T> resultBuilder, String message) {
        ApiException exception = new ApiException(message);
        return CompletableFuture.completedFuture(resultBuilder.apply(exception));
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return integrationActive ? super.acls(filter) : Collections.emptyList();
    }

    @Override
    public void close() {
        if (integrationActive) {
            super.close();
        }
    }
}
