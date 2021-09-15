/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class CustomAclBinding extends AclBinding {

    private static final Logger log = LoggerFactory.getLogger(CustomAclBinding.class);

    static final String USER_TYPE_PREFIX = KafkaPrincipal.USER_TYPE + ":";

    static final String DEFAULT = "default";
    static final String PRINCIPAL = "principal";
    static final String OPERATIONS = "operations";
    static final String OPERATIONS_EXCEPT = "operations-except";
    static final String APIS = "apis";
    static final String APIS_EXCEPT = "apis-except";
    static final String LISTENERS = "listeners";
    static final String PERMISSION = "permission";
    static final String PRIORITY = "priority";
    static final String WILDCARD = "*";

    private final Pattern resourceNamePattern;
    private final KafkaPrincipal principal;
    private final Pattern listenerPattern;
    private final int priority;

    public static List<AclBinding> valueOf(String configuration) {
        Boolean defaultBinding = null;
        ResourcePattern resourcePattern = null;
        String principal = null;
        AclPermissionType permission = null;
        Set<AclOperation> operations = null;
        boolean operationsExcluded = false;
        String listeners = null;
        Set<ApiKeys> apis = null;
        boolean apisExcluded = false;
        Integer priority = null;
        StringTokenizer st = new StringTokenizer(configuration, ";");

        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            int idx = token.indexOf('=');
            String k = token.substring(0, idx).toLowerCase();
            String v = token.substring(idx + 1).trim();

            switch (k) {
            case DEFAULT:
                requireNull(defaultBinding, DEFAULT);
                defaultBinding = Boolean.valueOf(v);
                break;

            case PRINCIPAL:
                requireNull(principal, PRINCIPAL);
                principal = preparePrincipal(v);
                break;

            case OPERATIONS:
                requireNull(operations, String.format("%s/%s", OPERATIONS, OPERATIONS_EXCEPT));
                operations = parseOperations(v, AclOperation.UNKNOWN, AclOperation.ANY);
                operationsExcluded = false;
                break;

            case OPERATIONS_EXCEPT:
                requireNull(operations, String.format("%s/%s", OPERATIONS, OPERATIONS_EXCEPT));
                operations = parseOperations(v, AclOperation.UNKNOWN, AclOperation.ANY, AclOperation.ALL);
                operationsExcluded = true;
                break;

            case APIS:
                requireNull(apis, String.format("%s/%s", APIS, APIS_EXCEPT));
                apis = parseApiKeys(v);
                apisExcluded = false;
                break;

            case APIS_EXCEPT:
                requireNull(apis, String.format("%s/%s", APIS, APIS_EXCEPT));
                apis = parseApiKeys(v);
                apisExcluded = true;
                break;

            case LISTENERS:
                requireNull(listeners, LISTENERS);
                listeners = v;
                break;

            case PERMISSION:
                requireNull(permission, PERMISSION);
                permission = AclPermissionType.fromString(v);
                validate("Permission type '" + v + "'", permission, AclPermissionType.UNKNOWN, AclPermissionType.ANY);
                break;

            case PRIORITY:
                requireNull(priority, PRIORITY);
                priority = Integer.valueOf(v);
                break;

            default:
                requireNull(resourcePattern, "resource type (" + v + ")");
                resourcePattern = parseResourcePattern(k, v);
                break;
            }
        }

        if (resourcePattern == null) {
            throw new IllegalArgumentException("ACL configuration missing resource type: '" + configuration + "'");
        }

        principal = value(principal, () -> USER_TYPE_PREFIX + WILDCARD);
        permission = value(permission, () -> AclPermissionType.ALLOW);
        operations = value(operations, () -> Collections.singleton(AclOperation.ALL));
        listeners = value(listeners, () -> WILDCARD);
        apis = value(apis, Collections::emptySet);
        priority = value(priority, () -> Integer.MAX_VALUE);

        if (Boolean.FALSE.equals(value(defaultBinding, () -> Boolean.FALSE))) {
            var entry = new ApiAwareAccessControlEntry(principal, WILDCARD, operations, operationsExcluded, apis, apisExcluded, permission);
            return Collections.singletonList(new CustomAclBinding(resourcePattern, entry, listeners, priority));
        }

        if (!apis.isEmpty()) {
            log.warn("APIs specified for default binding will be ignored by Kafka ACL processor: {}", configuration);
        }

        if (!WILDCARD.equals(listeners)) {
            log.warn("Listeners specified for default binding will be ignored by Kafka ACL processor: {}", configuration);
        }

        if (operationsExcluded) {
            throw new IllegalArgumentException("Default ACL binding may not list '" + OPERATIONS_EXCEPT + "' : '" + configuration + "'");
        }

        if (apisExcluded) {
            throw new IllegalArgumentException("Default ACL binding may not list '" + APIS_EXCEPT + "' : '" + configuration + "'");
        }

        String bindingPrincipal = principal;
        AclPermissionType permissionType = permission;
        ResourcePattern pattern = resourcePattern;

        return operations.stream()
                .map(operation -> new AccessControlEntry(bindingPrincipal, WILDCARD, operation, permissionType))
                .map(entry -> new AclBinding(pattern, entry))
                .collect(Collectors.toList());
    }

    static <T> void requireNull(T value, String label) {
        if (value != null) {
            throw new IllegalArgumentException("Duplicate ACL binding option specified: " + label);
        }
    }

    static String preparePrincipal(String principal) {
        if (principal.startsWith(USER_TYPE_PREFIX)) {
            return principal;
        }

        return USER_TYPE_PREFIX + principal;
    }

    static Set<AclOperation> parseOperations(String value, AclOperation... disallowedValues) {
        return splitOnComma(value).stream()
            .map(AclOperation::fromString)
            .filter(operation -> {
                validate("Operation type '" + value + "'", operation, (Object[]) disallowedValues);
                return true;
            })
            .collect(Collectors.toSet());
    }

    static Set<ApiKeys> parseApiKeys(String value) {
        return splitOnComma(value).stream()
            .map(String::toUpperCase)
            .map(ApiKeys::valueOf)
            .collect(Collectors.toSet());
    }

    static List<String> splitOnComma(String value) {
        if (value.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.asList(value.split("\\s*,\\s*"));
    }

    static ResourcePattern parseResourcePattern(String typeName, String value) {
        ResourceType type = ResourceType.fromString(typeName);
        validate("Resource type '" + typeName + "'", type, ResourceType.UNKNOWN);
        boolean usesGlob = value.length() > 1 && value.endsWith(WILDCARD);
        PatternType patternType = usesGlob ? PatternType.PREFIXED : PatternType.LITERAL;
        String name = usesGlob ? value.substring(0, value.length() - 1) : value; //Remove the glob, replaced with PREFIXED PatternType
        return new ResourcePattern(type, name, patternType);
    }

    static void validate(String label, Object value, Object... disallowedValues) {
        for (Object disallowed : disallowedValues) {
            if (value == disallowed) {
                throw new IllegalArgumentException(label + " is invalid or not supported");
            }
        }
    }

    static <T> T value(T value, Supplier<T> defaultValue) {
        return value != null ? value : defaultValue.get();
    }

    CustomAclBinding(ResourcePattern resource, ApiAwareAccessControlEntry entry, String listeners, int priority) {
        super(resource, entry);

        this.resourceNamePattern = resourceNamePattern(resource);
        this.principal = SecurityUtils.parseKafkaPrincipal(entry.principal());
        this.listenerPattern = parse(listeners);
        this.priority = priority;
    }

    static Pattern resourceNamePattern(ResourcePattern resource) {
        final String resourceName = resource.name();
        final PatternType type = resource.patternType();
        final Pattern result;

        if (type == PatternType.PREFIXED) {
            result = Pattern.compile("^" + Pattern.quote(resourceName) + ".*");
        } else if (ResourcePattern.WILDCARD_RESOURCE.equals(resourceName)) {
            result = Pattern.compile(".*");
        } else {
            result = Pattern.compile("^" + Pattern.quote(resourceName) + "$");
        }

        return result;
    }

    static Pattern parse(String patternString) {
        if (ResourcePattern.WILDCARD_RESOURCE.equals(patternString)) {
            return Pattern.compile(".*");
        }
        return Pattern.compile(patternString);
    }

    public int getPriority() {
        return priority;
    }

    public boolean matchesResource(String resourceName) {
        return match(() -> resourceNamePattern.matcher(resourceName).matches(), "resource.name", resourceNamePattern.pattern(), resourceName);
    }

    public boolean matchesPrincipal(KafkaPrincipal principal) {
        return match(() -> {
            if (!this.principal.getPrincipalType().equals(principal.getPrincipalType())) {
                return false;
            }

            return Arrays.asList(principal.getName(), WILDCARD)
                    .contains(this.principal.getName());
        }, PRINCIPAL, this.principal, principal);
    }

    public boolean isPrincipalSpecified() {
        return !WILDCARD.equals(principal.getName());
    }

    public boolean matchesOperation(AclOperation operation) {
        String fieldName = "operation";
        ApiAwareAccessControlEntry entry = (ApiAwareAccessControlEntry) entry();
        Set<AclOperation> operations = entry.operations();

        if (entry.operationsExcluded()) {
            return match(() -> !operations.contains(operation), fieldName, operations, operation);
        }

        return match(() -> operations.contains(AclOperation.ALL) || operations.contains(operation), fieldName, operations, operation);
    }

    public boolean matchesListener(String listenerName) {
        return match(() -> listenerPattern.matcher(listenerName).matches(), "listener", listenerPattern.pattern(), listenerName);
    }

    public boolean matchesApiKey(int apiKey) {
        String fieldName = "apiKey";
        ApiAwareAccessControlEntry entry = (ApiAwareAccessControlEntry) entry();
        Set<ApiKeys> apis = entry.apiKeys();

        if (entry.apiKeysExcluded()) {
            return match(() -> !apis.contains(ApiKeys.forId(apiKey)), fieldName, apis, apiKey);
        }

        return match(() -> apis.isEmpty() || apis.contains(ApiKeys.forId(apiKey)), fieldName, apis, apiKey);
    }

    boolean match(BooleanSupplier matcher, String fieldName, Object bindingValue, Object requestValue) {
        boolean matches = matcher.getAsBoolean();

        if (log.isTraceEnabled()) {
            if (matches) {
                log.trace("Binding attribute {} value `{}` matches request value `{}`", fieldName, bindingValue, requestValue);
            } else {
                log.trace("Binding attribute {} value `{}` does not match request value `{}`", fieldName, bindingValue, requestValue);
            }
        }

        return matches;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o)
                && getClass() == o.getClass()
                && listenerPattern == ((CustomAclBinding) o).listenerPattern
                && priority == ((CustomAclBinding) o).priority;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), listenerPattern, priority);
    }

    @Override
    public String toString() {
        return "(pattern=" + super.pattern() +
                ", entry=" + super.entry() +
                ", listenerPattern=" + listenerPattern.pattern() +
                ", priority=" + priority +
                ")";
    }
}
