package io.bf2.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import io.bf2.kafka.authorizer.AclLoggingConfig;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.security.Principal;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static io.bf2.kafka.authorizer.CustomAclAuthorizer.ACL_PREFIX;

public class LoggingController {
    private static final MessageFormat MESSAGE_FORMAT = new MessageFormat("Principal = {0} is {1} Operation = {2} from host = {3} via listener {4} on resource = {5}{6}{7}{8}{9} for request = {10} with resourceRefCount = {11}{12,choice,0#|1#|1< with {12,number,integer} identical entries suppressed}");
    private static final String LOGGING_PREFIX = ACL_PREFIX + "logging.";
    private static final Pattern ACL_LOGGING_PATTERN = Pattern.compile(Pattern.quote(LOGGING_PREFIX) + "\\d+");
    private final Cache<CacheKey, CacheEntry> loggingEventCache;
    private final Logger targetLogger;
    private final Map<ResourceType, List<AclLoggingConfig>> aclLoggingMap;
    private final Logger log = LoggerFactory.getLogger(LoggingController.class);

    public static Map<ResourceType, List<AclLoggingConfig>> extractLoggingConfiguration(Map<String, ?> configs) {
        final Map<ResourceType, List<AclLoggingConfig>> aclLoggingMap = new EnumMap<>(ResourceType.class);
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
        return aclLoggingMap;
    }

    public LoggingController(final Logger targetLogger, Map<ResourceType, List<AclLoggingConfig>> aclLoggingMap) {
        this.targetLogger = targetLogger;
        this.aclLoggingMap = aclLoggingMap;
        loggingEventCache = CacheBuilder.newBuilder()
                .expireAfterWrite(Duration.of(5000, ChronoUnit.SECONDS))
                .maximumSize(5000)
                .removalListener((RemovalListener<CacheKey, CacheEntry>) removalNotification ->
                        removalNotification.getValue().log())
                .build();
    }

    public void logAuditMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        if ((authorized && action.logIfAllowed()) ||
                (!authorized && action.logIfDenied())) {
            final LoggingController.CacheKey cacheKey = new LoggingController.CacheKey(action.resourcePattern().name(),
                    action.operation(),
                    requestContext.principal(),
                    requestContext.requestType(),
                    requestContext.listenerName(),
                    authorized);
            try {
                final CacheEntry cacheEntry = loggingEventCache.get(cacheKey, () -> new CacheEntry(
                        logLevelFor(requestContext, action),
                        (suppressedCount) -> buildLogMessage(requestContext, action, authorized, suppressedCount)));
                cacheEntry.suppressionCounter.increment();
            } catch (ExecutionException e) {
                log.error("Unable to read log event cache. {e}", e);
                logAtAllowedLevel(logLevelFor(requestContext, action),
                        () -> buildLogMessage(requestContext, action, authorized, 0L));
            }
        } else if (log.isTraceEnabled()) {
            log.trace(buildLogMessage(requestContext, action, authorized, 0L));
        }
    }

    public Level logLevelFor(AuthorizableRequestContext requestContext, Action action) {
        return aclLoggingMap.getOrDefault(action.resourcePattern().resourceType(), Collections.emptyList())
                .stream()
                .filter(binding -> binding.matchesResource(action.resourcePattern().name())
                        && binding.matchesOperation(action.operation())
                        && binding.matchesPrincipal(requestContext.principal())
                        && binding.matchesApiKey(requestContext.requestType())
                        && binding.matchesListener(requestContext.listenerName()))
                .min(AclLoggingConfig::prioritize)
                .map(AclLoggingConfig::getLevel)
                .orElse(Level.INFO);
    }

    public void logAtAllowedLevel(Level lvl, Supplier<String> msg) {
        switch (lvl) {
            case ERROR:
                if (targetLogger.isErrorEnabled()) {
                    targetLogger.error(msg.get());
                }
                break;
            case WARN:
                if (targetLogger.isWarnEnabled()) {
                    targetLogger.warn(msg.get());
                }
                break;
            case INFO:
                if (targetLogger.isInfoEnabled()) {
                    targetLogger.info(msg.get());
                }
                break;
            case DEBUG:
                if (targetLogger.isDebugEnabled()) {
                    targetLogger.debug(msg.get());
                }
                break;
            case TRACE:
                if (targetLogger.isTraceEnabled()) {
                    targetLogger.trace(msg.get());
                }
                break;
        }
    }

    public String buildLogMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized, Long suppressedCount) {
        Principal principal = requestContext.principal();
        String operation = SecurityUtils.operationName(action.operation());
        String host = requestContext.clientAddress().getHostAddress();
        String listenerName = requestContext.listenerName();
        String resourceType = SecurityUtils.resourceTypeName(action.resourcePattern().resourceType());
        String authResult = authorized ? "Allowed" : "Denied";
        Object apiKey = ApiKeys.hasId(requestContext.requestType()) ? ApiKeys.forId(requestContext.requestType()).name() : requestContext.requestType();
        int refCount = action.resourceReferenceCount();
        return MESSAGE_FORMAT.format(new Object[]{principal,
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
                refCount,
                suppressedCount});
    }

    @VisibleForTesting
    void evictWindowedEvents() {
        loggingEventCache.invalidateAll();
    }

    public static class CacheKey {
        private final String resourceName;
        private final AclOperation operation;
        private final KafkaPrincipal principal;
        private final int requestType;
        private final String listenerName;
        private final boolean authorized;

        private CacheKey(String resourceName, AclOperation operation, KafkaPrincipal principal, int requestType, String listenerName, boolean authorized) {
            this.resourceName = resourceName;
            this.operation = operation;
            this.principal = principal;
            this.requestType = requestType;
            this.listenerName = listenerName;
            this.authorized = authorized;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return requestType == cacheKey.requestType &&
                    authorized == cacheKey.authorized &&
                    Objects.equals(resourceName, cacheKey.resourceName) &&
                    operation == cacheKey.operation &&
                    Objects.equals(principal, cacheKey.principal) &&
                    Objects.equals(listenerName, cacheKey.listenerName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceName, operation, principal, requestType, listenerName, authorized);
        }
    }

    private class CacheEntry {
        private final LongAdder suppressionCounter;
        private final Level logLevel;
        private final Function<Long, String> messageGenerator;

        private CacheEntry(Level logLevel, Function<Long, String> messageGenerator) {
            this.suppressionCounter = new LongAdder();
            this.logLevel = logLevel;
            this.messageGenerator = messageGenerator;
        }

        public void log() {
            logAtAllowedLevel(logLevel, () -> messageGenerator.apply(suppressionCounter.longValue()));
        }
    }

}
