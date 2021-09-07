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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class CustomAclBinding extends AclBinding {

    private static final Logger log = LoggerFactory.getLogger(CustomAclBinding.class);

    static final String USER_TYPE_PREFIX = KafkaPrincipal.USER_TYPE + ":";

    static final String DEFAULT = "default";
    static final String OVERRIDE = "override";
    static final String PRINCIPAL = "principal";
    static final String OPERATIONS = "operations";
    static final String APIS = "apis";
    static final String LISTENERS = "listeners";
    static final String PERMISSION = "permission";
    static final String WILDCARD = "*";

    private final Pattern resourceNamePattern;
    private final KafkaPrincipal principal;
    private final Pattern listenerPattern;
    private final Set<ApiKeys> apiKeys;
    private final Category category;

    public enum Category {
        /**
         * Category of bindings that will be created as Kafka ACLs if no other Kafka ACLs
         * exist upon broker start-up.
         */
        DEFAULT_BINDING,
        /**
         * Category of bindings that will be processed following delegation to the Kafka AclAuthorizer only when
         * the authorizer allows the action. Bindings in this category are meant to prevent authorization of certain
         * APIs, a level of granularity not supported by Kafka ACLs.
         */
        OVERRIDE_BINDING,
        /**
         * Category of bindings that will be process before delegating to the Kafka AclAuthorizer.
         */
        PREDELEGATION_BINDING
    }

    public static List<CustomAclBinding> valueOf(String configuration) {
        boolean defaultBinding = false;
        boolean overrideBinding = false;
        ResourcePattern resourcePattern = null;
        String principal = WILDCARD;
        AclPermissionType permissionType = AclPermissionType.ALLOW;
        List<AclOperation> operations = new ArrayList<>();
        String listeners = WILDCARD;
        Set<ApiKeys> apiKeys = EnumSet.noneOf(ApiKeys.class);
        StringTokenizer st = new StringTokenizer(configuration, ";");

        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            int idx = token.indexOf('=');
            String k = token.substring(0, idx).toLowerCase();
            String v = token.substring(idx+1).trim();

            switch (k) {
            case DEFAULT:
                defaultBinding = Boolean.valueOf(v);
                break;

            case OVERRIDE:
                overrideBinding = Boolean.valueOf(v);
                break;

            case PRINCIPAL:
                principal = v;
                break;

            case OPERATIONS:
                splitOnComma(v).stream()
                    .map(AclOperation::fromString)
                    .filter(operation -> {
                        validate("Operation type '" + v + "'", operation, AclOperation.UNKNOWN, AclOperation.ANY);
                        return true;
                    })
                    .forEach(operations::add);
                break;

            case APIS:
                splitOnComma(v).stream()
                    .map(String::toUpperCase)
                    .map(ApiKeys::valueOf)
                    .forEach(apiKeys::add);
                break;

            case LISTENERS:
                listeners = v;
                break;

            case PERMISSION:
                permissionType = AclPermissionType.fromString(v);
                validate("Permission Type '" + v + "'", permissionType, AclPermissionType.UNKNOWN, AclPermissionType.ANY);
                break;

            default:
                ResourceType type = ResourceType.fromString(k);
                validate("Resource type '" + k + "'", type, ResourceType.UNKNOWN);
                boolean usesGlob = v.length() > 1 && v.endsWith("*");
                PatternType patternType = usesGlob ? PatternType.PREFIXED : PatternType.LITERAL;
                String name = usesGlob ? v.substring(0, v.length() - 1) : v; //Remove the glob, replaced with PREFIXED PatternType
                resourcePattern = new ResourcePattern(type, name, patternType);
                break;
            }
        }

        if (resourcePattern == null) {
            throw new IllegalArgumentException("ACL configuration missing resource type: '" + configuration + "'");
        }

        final Category category;

        if (defaultBinding) {
            category = Category.DEFAULT_BINDING;
        } else if (overrideBinding) {
            category = Category.OVERRIDE_BINDING;
        } else {
            category = Category.PREDELEGATION_BINDING;
        }

        return buildBindings(operations, principal, resourcePattern, listeners, apiKeys, permissionType, category);
    }

    static List<String> splitOnComma(String value) {
        if (value.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.asList(value.split("\\s*,\\s*"));
    }

    static void validate(String label, Object value, Object... disallowedValues) {
        for (Object disallowed : disallowedValues) {
            if (value == disallowed) {
                throw new IllegalArgumentException(label + " is invalid or not supported");
            }
        }
    }

    static List<CustomAclBinding> buildBindings(List<AclOperation> operations,
            String principal,
            ResourcePattern resource,
            String listeners,
            Set<ApiKeys> apiKeys,
            AclPermissionType permission,
            Category category) {

        final String bindingPrincipal;

        if (!principal.startsWith(USER_TYPE_PREFIX)) {
            bindingPrincipal = USER_TYPE_PREFIX + principal;
        } else {
            bindingPrincipal = principal;
        }

        return operations.stream()
            .map(operation -> new AccessControlEntry(bindingPrincipal, WILDCARD, operation, permission))
            .map(entry -> new CustomAclBinding(resource, entry, listeners, apiKeys, category))
            .collect(Collectors.toList());
    }

    CustomAclBinding(ResourcePattern resource, AccessControlEntry entry, String listeners, Set<ApiKeys> apiKeys, Category category) {
        super(resource, entry);

        this.resourceNamePattern = resourceNamePattern(resource);
        this.principal = SecurityUtils.parseKafkaPrincipal(entry.principal());
        this.listenerPattern = parse(listeners);
        this.apiKeys = apiKeys.isEmpty() ? Collections.emptySet() : EnumSet.copyOf(apiKeys);
        this.category = category;
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

    public Category getCategory() {
        return category;
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
        AclOperation allowedOperation = entry().operation();
        return match(() -> Arrays.asList(operation, AclOperation.ALL).contains(allowedOperation), "operation", allowedOperation, operation);
    }

    public boolean matchesListener(String listenerName) {
        return match(() -> listenerPattern.matcher(listenerName).matches(), "listener", listenerPattern.pattern(), listenerName);
    }

    public boolean matchesApiKey(int apiKey) {
        return match(() -> apiKeys.isEmpty() || apiKeys.contains(ApiKeys.forId(apiKey)), "apiKey", apiKeys, apiKey);
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
        return super.equals(o) &&
                Objects.equals(apiKeys, ((CustomAclBinding) o).apiKeys) &&
                category == ((CustomAclBinding) o).category;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), apiKeys, category);
    }

    @Override
    public String toString() {
        return "(pattern=" + super.pattern() +
                ", entry=" + super.entry() +
                ", listenerPattern=" + listenerPattern.pattern() +
                ", apiKeys=" + apiKeys +
                ", category=" + category + ")";
    }
}
