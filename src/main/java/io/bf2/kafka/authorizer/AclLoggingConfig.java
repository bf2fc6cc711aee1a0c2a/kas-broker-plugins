/*
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.function.BooleanSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AclLoggingConfig {

    private static final Logger log = LoggerFactory.getLogger(AclLoggingConfig.class);

    static final String LEVEL = "level";

    private final Level level;
    private final ResourcePattern resourcePattern;
    private final Pattern resourceNamePattern;
    private final KafkaPrincipal principal;
    private final Pattern listenerPattern;
    private final Set<AclOperation> operations;
    private final boolean operationsExcluded;
    private final Set<ApiKeys> apiKeys;
    private final boolean apiKeysExcluded;
    private final Integer priority;

    public AclLoggingConfig(Level level, ResourcePattern resourcePattern, String listeners,
            Set<AclOperation> operations, boolean operationsExcluded, String principal, Set<ApiKeys> apis,
            boolean apiKeysExcluded, Integer priority) {
        this.level = level;
        this.resourcePattern = resourcePattern;
        this.resourceNamePattern = CustomAclBinding.resourceNamePattern(resourcePattern);
        this.listenerPattern = CustomAclBinding.parse(listeners);
        this.principal = SecurityUtils.parseKafkaPrincipal(principal);
        this.operations = operations;
        this.operationsExcluded = operationsExcluded;
        this.apiKeys = apis;
        this.apiKeysExcluded = apiKeysExcluded;
        this.priority = priority;
    }

    public static List<AclLoggingConfig> valueOf(String configuration) {
        Level logLevel = null;
        String principal = null;
        String listeners = null;
        Integer priority = null;
        Set<ApiKeys> apis = null;
        boolean apisExcluded = false;
        boolean operationsExcluded = false;
        Set<AclOperation> operations = null;
        ResourcePattern resourcePattern = null;
        StringTokenizer st = new StringTokenizer(configuration, ";");

        while (st.hasMoreTokens()) {
            String token = st.nextToken();
            int idx = token.indexOf('=');
            String k = token.substring(0, idx).toLowerCase();
            String v = token.substring(idx + 1).trim();

            switch (k) {
            case CustomAclBinding.PRINCIPAL:
                CustomAclBinding.requireNull(principal, CustomAclBinding.PRINCIPAL);
                principal = CustomAclBinding.preparePrincipal(v);
                break;

            case CustomAclBinding.OPERATIONS:
                CustomAclBinding.requireNull(operations, String.format("%s/%s", CustomAclBinding.OPERATIONS, CustomAclBinding.OPERATIONS_EXCEPT));
                operations = parseOperations(v);
                operationsExcluded = false;
                break;

            case CustomAclBinding.OPERATIONS_EXCEPT:
                CustomAclBinding.requireNull(operations, String.format("%s/%s", CustomAclBinding.OPERATIONS, CustomAclBinding.OPERATIONS_EXCEPT));
                operations = parseOperations(v);
                operationsExcluded = true;
                break;

            case CustomAclBinding.APIS:
                CustomAclBinding.requireNull(apis, String.format("%s/%s", CustomAclBinding.APIS, CustomAclBinding.APIS_EXCEPT));
                apis = CustomAclBinding.parseApiKeys(v);
                apisExcluded = false;
                break;

            case CustomAclBinding.APIS_EXCEPT:
                CustomAclBinding.requireNull(apis, String.format("%s/%s", CustomAclBinding.APIS, CustomAclBinding.APIS_EXCEPT));
                apis = CustomAclBinding.parseApiKeys(v);
                apisExcluded = true;
                break;

            case CustomAclBinding.LISTENERS:
                CustomAclBinding.requireNull(listeners, CustomAclBinding.LISTENERS);
                listeners = v;
                break;

            case CustomAclBinding.PRIORITY:
                CustomAclBinding.requireNull(priority, CustomAclBinding.PRIORITY);
                priority = Integer.valueOf(v);
                break;

            case LEVEL:
                CustomAclBinding.requireNull(logLevel, LEVEL);
                logLevel = Level.valueOf(v);
                break;

            default:
                CustomAclBinding.requireNull(resourcePattern, "resource type (" + v + ")");
                resourcePattern = CustomAclBinding.parseResourcePattern(k, v);
                break;
            }
        }

        if (resourcePattern == null) {
            throw new IllegalArgumentException("ACL configuration missing resource type: '" + configuration + "'");
        }

        principal = CustomAclBinding.value(principal, () -> CustomAclBinding.USER_TYPE_PREFIX + CustomAclBinding.WILDCARD);
        operations = CustomAclBinding.value(operations, () -> Collections.singleton(AclOperation.ALL));
        listeners = CustomAclBinding.value(listeners, () -> CustomAclBinding.WILDCARD);
        apis = CustomAclBinding.value(apis, Collections::emptySet);
        priority = CustomAclBinding.value(priority, () -> Integer.MAX_VALUE);
        logLevel = CustomAclBinding.value(logLevel, () -> Level.INFO);

        return Collections.singletonList(
                new AclLoggingConfig(logLevel, resourcePattern, listeners, operations, operationsExcluded, principal,
                        apis, apisExcluded, priority));
    }

    static int prioritize(AclLoggingConfig b1, AclLoggingConfig b2) {
        int priorityComparison = Integer.compare(b1.getPriority(), b2.getPriority());

        if (priorityComparison != 0) {
            return priorityComparison;
        }

        // Inverted sort order, because the int values for Levels go the other way than we want
        return Integer.compare(b2.getLevel().toInt(), b1.getLevel().toInt());
    }

    public Level getLevel() {
        return level;
    }

    public Integer getPriority() {
        return priority;
    }

    public ResourcePattern getResourcePattern() {
        return resourcePattern;
    }

    static Set<AclOperation> parseOperations(String value) {
        return CustomAclBinding.splitOnComma(value).stream().map(AclOperation::fromString).collect(Collectors.toSet());
    }

    public boolean matchesResource(String resourceName) {
        return match(() -> resourceNamePattern.matcher(resourceName).matches(), "resource.name",
                resourceNamePattern.pattern(), resourceName);
    }

    public boolean matchesPrincipal(KafkaPrincipal principal) {
        return match(() -> {
            if (!this.principal.getPrincipalType().equals(principal.getPrincipalType())) {
                return false;
            }

            return Arrays.asList(principal.getName(), CustomAclBinding.WILDCARD)
                    .contains(this.principal.getName());
        }, CustomAclBinding.PRINCIPAL, this.principal, principal);
    }

    public boolean matchesOperation(AclOperation operation) {
        String fieldName = "operation";
        Set<AclOperation> operations = this.operations;

        if (this.operationsExcluded) {
            return match(() -> !operations.contains(operation), fieldName, operations, operation);
        }

        return match(() -> operations.contains(AclOperation.ALL) || operations.contains(operation), fieldName,
                operations, operation);
    }

    public boolean matchesListener(String listenerName) {
        return match(() -> listenerPattern.matcher(listenerName).matches(), "listener", listenerPattern.pattern(),
                listenerName);
    }

    public boolean matchesApiKey(int apiKey) {
        String fieldName = "apiKey";
        Set<ApiKeys> apis = this.apiKeys;

        if (this.apiKeysExcluded) {
            return match(() -> !apis.contains(ApiKeys.forId(apiKey)), fieldName, apis, apiKey);
        }

        return match(() -> apis.isEmpty() || apis.contains(ApiKeys.forId(apiKey)), fieldName, apis, apiKey);
    }

    boolean match(BooleanSupplier matcher, String fieldName, Object bindingValue, Object requestValue) {
        boolean matches = matcher.getAsBoolean();

        if (log.isTraceEnabled()) {
            if (matches) {
                log.trace("Binding attribute {} value `{}` matches request value `{}`", fieldName, bindingValue,
                        requestValue);
            } else {
                log.trace("Binding attribute {} value `{}` does not match request value `{}`", fieldName, bindingValue,
                        requestValue);
            }
        }

        return matches;
    }
}
