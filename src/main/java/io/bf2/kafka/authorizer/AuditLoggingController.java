package io.bf2.kafka.authorizer;

import com.google.common.annotations.VisibleForTesting;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.bf2.kafka.authorizer.CustomAclAuthorizer.ACL_PREFIX;

public class AuditLoggingController implements Configurable {

    private static final Logger log = LoggerFactory.getLogger(AuditLoggingController.class);
    private static final String LOGGING_PREFIX = ACL_PREFIX + "logging.";
    private static final Pattern ACL_LOGGING_PATTERN = Pattern.compile(Pattern.quote(LOGGING_PREFIX) + "\\d+");

    @VisibleForTesting
    final Map<ResourceType, List<AclLoggingConfig>> aclLoggingMap = new EnumMap<>(ResourceType.class);

    @Override
    public void configure(Map<String, ?> configs) {
        configs.entrySet()
                .stream()
                .filter(config -> ACL_LOGGING_PATTERN.matcher(config.getKey()).matches())
                .map(Map.Entry::getValue)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .map(AclLoggingConfig::valueOf)
                .flatMap(List::stream)
                .forEach(binding -> {
                    aclLoggingMap.compute(binding.getResourcePattern().resourceType(), (k, v) -> {
                        List<AclLoggingConfig> bindings = Objects.requireNonNullElseGet(v, ArrayList::new);
                        bindings.add(binding);
                        return bindings;
                    });
                });

    }

    public void logAuditMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        if ((authorized && action.logIfAllowed()) ||
                (!authorized && action.logIfDenied())) {
            logAtAllowedLevel(logLevelFor(requestContext, action),
                    () -> buildLogMessage(requestContext, action, authorized));
        } else if (log.isTraceEnabled()) {
            log.trace(buildLogMessage(requestContext, action, authorized));
        }
    }

    Level logLevelFor(AuthorizableRequestContext requestContext, Action action) {
        return aclLoggingMap.getOrDefault(action.resourcePattern().resourceType(), Collections.emptyList())
                .stream()
                .filter(binding -> binding.matchesResource(action.resourcePattern().name())
                        && binding.matchesOperation(action.operation())
                        && binding.matchesPrincipal(requestContext.principal())
                        && binding.matchesApiKey(requestContext.requestType())
                        && binding.matchesListener(requestContext.listenerName()))
                .sorted(AclLoggingConfig::prioritize)
                .findFirst()
                .map(AclLoggingConfig::getLevel)
                .orElse(Level.INFO);
    }

    void logAtAllowedLevel(Level lvl, Supplier<String> msg) {
        switch (lvl) {
            case ERROR:
                if (log.isErrorEnabled()) {
                    log.error(msg.get());
                }
                break;
            case WARN:
                if (log.isWarnEnabled()) {
                    log.warn(msg.get());
                }
                break;
            case INFO:
                if (log.isInfoEnabled()) {
                    log.info(msg.get());
                }
                break;
            case DEBUG:
                if (log.isDebugEnabled()) {
                    log.debug(msg.get());
                }
                break;
            case TRACE:
                if (log.isTraceEnabled()) {
                    log.trace(msg.get());
                }
                break;
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

    public void logAtLevel(AuthorizableRequestContext requestContext, Action action, String prefox, boolean authorized) {
        logAtAllowedLevel(logLevelFor(requestContext, action), () -> prefox + buildLogMessage(requestContext, action, authorized));
    }

}
