/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.bf2.kafka.common.PartitionCounter;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.stream.IntStream;

/**
 * A authorizer for Kafka that defines custom ACLs. The configuration is provided as
 * a string a semicolon-delimited key/value pairs to specify
 *
 * <ol>
 * <li><code>permission</code> - either <code>allow</code> or <code>deny</code>.
 *   If not specified, defaults to <code>allow</code>.
 * <li><code>principal</code> - may be either a principal name or wildcard <code>*</code>,
 *   indicating the user(s) for which the ACL is applicable. If not specified,
 *   defaults to <code>*</code> and the ACL applies to all users.
 * <li>resource - a key/value pair where the key is one of the enumerated resource
 *   types (see {@link ResourceType}) and the value is the resource name. A wildcard
 *   <code>*</code> may be used to match any resource of the associated type. If
 *   the resource name is not a single wildcard (<code>*</code>) and ends with an
 *   asterisk/glob, the type of pattern is considered to be
 *   {@link PatternType#PREFIXED PREFIXED}, otherwise it is considered to be
 *   {@link PatternType#LITERAL LITERAL}
 * <li><code>operations</code> - comma-delimited list of operations for the
 *   ACL binding. See {@link AclOperation} for the enumeration names.
 * <li><code>operations-except</code> - comma-delimited list of operations that do
 *   not apply for the ACL binding. See {@link AclOperation} for the enumeration names.
 * <li><code>apis</code> - comma-delimited list of APIs to further focus the ACL
 *   binding. See {@link ApiKeys} for the enumeration names.
 * <li><code>apis-except</code> - comma-delimited list of excluded APIs to further
 *   focus the ACL binding. See {@link ApiKeys} for the enumeration names.
 * <li><code>listeners</code> - a regular expression used to match the listener
 *   name used to make a request. If specified, the ACL will only be used with
 *   requests made via a matching listener.
 * <li><code>default</code> - boolean value to identify an ACL binding as a default
 *   binding. Default bindings will be saved as Kafka ACLs when no other
 *   Kafka ACLs have been configured.
 * </ol>
 *
 * Examples:
 * <p>Allow user <code>admin</code> access to read, write, and create topics
 *    prefixed with <code>foo</code>.
 * <pre>
 * acl.1: permission=allow;principal=admin;topic=foo*;operations=read,write,create
 * </pre>
 * <p>Allow all users access to read a topic named exactly <code>bar</code>.
 * <pre>
 * acl.2: permission=allow;topic=bar;operations=read
 * </pre>
 * <p>Deny all users access to read and create consumer groups named <code>xyz</code>
 *    when connecting via a listener with a name starting with <code>internal</code>.
 * <pre>
 * acl.3: permission=deny;listener=internal.*;group=xyz;operations=read,create
 * </pre>
 */
public class CustomAclAuthorizer implements Authorizer {

    private static final int CREATE_PARTITIONS_APIKEY = 37;
    private static final Logger log = LoggerFactory.getLogger(CustomAclAuthorizer.class);

    static final String CREATE_ACL_INVALID_PRINCIPAL = "Invalid ACL principal name";
    static final String CREATE_ACL_INVALID_BINDING = "Invalid ACL resource or operation";

    static final String CONFIG_PREFIX = "strimzi.authorization.custom-authorizer.";
    static final String RESOURCE_OPERATIONS_KEY = CONFIG_PREFIX + "resource-operations";

    static final ResourcePatternFilter ANY_RESOURCE = new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY);
    static final AccessControlEntryFilter ANY_ENTRY = new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY);
    static final AclBindingFilter ANY_ACL = new AclBindingFilter(ANY_RESOURCE, ANY_ENTRY);

    /**
     * For backward-compatibility with {@link GlobalAclAuthorizer}.
     *
     * @deprecated switch to a custom authenticated user instead
     */
    @Deprecated(forRemoval = true)
    static final String ALLOWED_LISTENERS = CONFIG_PREFIX + "allowed-listeners";
    static final String ACL_PREFIX = CONFIG_PREFIX + "acl.";
    static final Pattern ACL_PATTERN = Pattern.compile(Pattern.quote(ACL_PREFIX) + "\\d+");

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

    final kafka.security.authorizer.AclAuthorizer delegate;
    final AuditLoggingController loggingController;

    private volatile int maxPartitions;
    PartitionCounter partitionCounter;

    public CustomAclAuthorizer(kafka.security.authorizer.AclAuthorizer delegate) {
        this.delegate = delegate;
        this.loggingController = new AuditLoggingController();
    }

    public CustomAclAuthorizer() {
        this(new kafka.security.authorizer.AclAuthorizer());
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
        return delegate.start(serverInfo);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        delegate.configure(configs);
        loggingController.configure(configs);

        addAllowedListeners(configs);

        partitionCounter = new PartitionCounter(configs);
        partitionCounter.startPeriodicCounter(PartitionCounter.DEFAULT_SCHEDULE_PERIOD_MILLIS);

        maxPartitions = PartitionCounter.getMaxPartitions(configs);

        if (configs.containsKey(RESOURCE_OPERATIONS_KEY)) {
            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String, List<String>>> typeRef = new TypeReference<>() {};

            try {
                allowedAcls.putAll(mapper.readValue(String.valueOf(configs.get(RESOURCE_OPERATIONS_KEY)), typeRef));
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(RESOURCE_OPERATIONS_KEY, e);
            }
        }

        log.info("Allowed Custom Group Authorizer Listeners {}", this.allowedListeners);
        final List<AclBinding> defaultBindings = new ArrayList<>();

        // read custom ACLs configured for rest of the users
        configs.entrySet()
            .stream()
            .filter(config -> ACL_PATTERN.matcher(config.getKey()).matches())
            .map(Map.Entry::getValue)
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .map(CustomAclBinding::valueOf)
            .flatMap(List::stream)
            .forEach(binding -> {
                if (binding instanceof CustomAclBinding) {
                    CustomAclBinding custom = (CustomAclBinding) binding;

                    aclMap.compute(binding.pattern().resourceType(), (k, v) -> {
                        List<CustomAclBinding> bindings = Objects.requireNonNullElseGet(v, ArrayList::new);
                        bindings.add(custom);
                        return bindings;
                    });

                    if (custom.isPrincipalSpecified()) {
                        aclPrincipals.add(binding.entry().principal());
                    }
                } else {
                    defaultBindings.add(binding);
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

        configureDefaults(defaultBindings);
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

    void configureDefaults(List<AclBinding> defaultBindings) {
        final boolean aclsConfigured = delegate.acls(ANY_ACL).iterator().hasNext();
        final int defaultBindingCount = defaultBindings.size();

        if (defaultBindingCount > 0 && !aclsConfigured) {
            var bindingsConfigured = new ArrayList<>();
            var createResults = delegate.createAcls(null, defaultBindings);

            var pendingCompletion = IntStream.range(0, createResults.size())
                .mapToObj(i -> {
                    var binding = defaultBindings.get(i);
                    var stage = createResults.get(i);

                    return stage.whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("Failed to configure default ACL in AclAuthorizer: [{}]", binding, error);
                        } else if (result.exception().isPresent()) {
                            log.error("Failed to configure default ACL in AclAuthorizer: [{}]", binding, result.exception().get());
                        } else {
                            bindingsConfigured.add(binding);
                        }
                    }).toCompletableFuture();
                })
                .toArray(CompletableFuture<?>[]::new);

            CompletableFuture.allOf(pendingCompletion)
                .whenComplete((nothing, error) -> {
                    if (error != null) {
                        log.error("Failed to configure default ACLs in AclAuthorizer:\n\t{}",
                                 defaultBindings.stream()
                                     .map(Object::toString)
                                     .collect(Collectors.joining(",\n\t")),
                                 error);
                    } else if (log.isInfoEnabled()) {
                        log.info("Default ACLs configured in AclAuthorizer:\n\t{}",
                                 bindingsConfigured.stream()
                                     .map(Object::toString)
                                     .collect(Collectors.joining(",\n\t")));
                    }
                })
                .join();
        } else {
            log.info("Default ACLs not configured, aclsConfigured={}, defaultBindingCount={}",
                     aclsConfigured, defaultBindingCount);
        }
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        return actions.stream()
            .map(action -> authorizeAction(requestContext, action))
            .collect(Collectors.toList());
    }

    private AuthorizationResult authorizeAction(AuthorizableRequestContext requestContext, Action action) {
        if ((requestContext.requestType() == CREATE_PARTITIONS_APIKEY && maxPartitions > 0)
                && (partitionCounter.existingPartitionCount.get() >= maxPartitions)) {
            loggingController.logAtLevel(requestContext, action, "reached partition limit ", false);
            return AuthorizationResult.DENIED;
        }

        // is super user allow any operation
        if (delegate.isSuperUser(requestContext.principal())) {
            loggingController.logAtLevel(requestContext, action, "super.user ", true);
            return AuthorizationResult.ALLOWED;
        }

        // if request made on any allowed listeners allow always
        if (isAllowedListener(requestContext.listenerName())) {
            loggingController.logAtLevel(requestContext, action, "allowed listener ", true);
            return AuthorizationResult.ALLOWED;
        }

        if (log.isDebugEnabled()) {
            log.debug("User {} asking for permission {} using listener {}",
                    requestContext.principal().getName(),
                    action.operation(),
                    requestContext.listenerName());
        }

        List<CustomAclBinding> bindings =
                aclMap.getOrDefault(action.resourcePattern().resourceType(), Collections.emptyList());

        return fetchAuthorization(requestContext, action, bindings)
                .orElseGet(() -> delegateOrDeny(requestContext, action));
    }

    Optional<AuthorizationResult> fetchAuthorization(AuthorizableRequestContext requestContext, Action action, List<CustomAclBinding> bindings) {
        return bindings.stream()
                .filter(binding -> binding.matchesResource(action.resourcePattern().name()))
                .filter(binding -> binding.matchesOperation(action.operation()))
                .filter(binding -> binding.matchesApiKey(requestContext.requestType()))
                .filter(binding -> binding.matchesPrincipal(requestContext.principal()))
                .filter(binding -> binding.matchesListener(requestContext.listenerName()))
                .map(this::logCandidate)
                .sorted(CustomAclBinding::prioritize)
                .findFirst()
                .map(binding -> resultFromBinding(requestContext, action, binding));
    }

    CustomAclBinding logCandidate(CustomAclBinding binding) {
        if (log.isDebugEnabled()) {
            log.debug("Candidate ACL binding: {}", binding);
        }
        return binding;
    }

    AuthorizationResult resultFromBinding(AuthorizableRequestContext requestContext, Action action, AclBinding binding) {
        AuthorizationResult result = permissionResults.get(binding.entry().permissionType());
        loggingController.logAuditMessage(requestContext, action, result == AuthorizationResult.ALLOWED);
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
        boolean principalConfigured = hasPrincipalBindings(toString(requestContext.principal()));

        if (log.isTraceEnabled()) {
            log.trace("Default action: principalConfigured={}", principalConfigured);
        }

        if (principalConfigured) {
            loggingController.logAuditMessage(requestContext, action, false);
            return AuthorizationResult.DENIED;
        }

        // Indeterminate result - delegate to default ACL handling
        AuthorizationResult result = delegate.authorize(requestContext, List.of(action)).get(0);
        loggingController.logAuditMessage(requestContext, action, AuthorizationResult.ALLOWED.equals(result));
        return result;
    }

    boolean hasPrincipalBindings(String principalName) {
        return aclPrincipals.contains(principalName);
    }

    static String toString(KafkaPrincipal principal) {
        return principal.getPrincipalType() + ":" + principal.getName();
    }

    // openshift format PLAIN-9092://0.0.0.0:9092,OPEN-9093://0.0.0.0:9093,SRE-9096://0.0.0.0:9096
    // minikube PLAIN-9092,OPEN-9093,SRE-9096
    public boolean isAllowedListener(String listener) {
        return allowedListeners.stream().anyMatch(listener::startsWith);
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

                if (!binding.entry().principal().startsWith(CustomAclBinding.USER_TYPE_PREFIX)) {
                    /* Reject ACL operations as invalid where the principal named in the ACL binding is the principal performing the operation */
                    log.info("Rejected attempt by user {} to create ACL binding with invalid principal name: {}",
                            requestContext.principal().getName(),
                            binding.entry().principal());
                    result = errorResult(AclCreateResult::new, CREATE_ACL_INVALID_PRINCIPAL);
                } else if (Objects.equals(toString(requestContext.principal()), binding.entry().principal())) {
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
                    result = delegate.createAcls(requestContext, List.of(binding)).get(0);
                }

                return result;
            })
            .collect(Collectors.toList());
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {

        return delegate.deleteAcls(requestContext, aclBindingFilters);
    }

    <T> CompletionStage<T> errorResult(Function<ApiException, T> resultBuilder, String message) {
        ApiException exception = new InvalidRequestException(message);
        return CompletableFuture.completedFuture(resultBuilder.apply(exception));
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return delegate.acls(filter);
    }

    @Override
    public void close() throws IOException {
        if (delegate != null) {
            delegate.close();
        }

        if (loggingController != null) {
            loggingController.close();
        }

        if (partitionCounter != null) {
            partitionCounter.close();
        }

    }
}
