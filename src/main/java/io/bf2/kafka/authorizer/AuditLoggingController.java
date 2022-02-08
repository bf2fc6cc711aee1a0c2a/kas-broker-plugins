package io.bf2.kafka.authorizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.Closeable;
import java.security.Principal;
import java.sql.Date;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static io.bf2.kafka.authorizer.CustomAclAuthorizer.ACL_PREFIX;

public class AuditLoggingController implements Configurable, Closeable {

    private static final Logger log = LoggerFactory.getLogger(AuditLoggingController.class);
    private static final Logger auditLogger = LoggerFactory.getLogger("AuditEvents");
    private static final String LOGGING_PREFIX = ACL_PREFIX + "logging.";
    private static final Pattern ACL_LOGGING_PATTERN = Pattern.compile(Pattern.quote(LOGGING_PREFIX) + "\\d+");
    private static final Splitter CSV_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();
    private static final MessageFormat MESSAGE_FORMAT = new MessageFormat("Principal = {0} is {1} Operation = {2} from host = {3} via listener {4} on resource = {5}{6}{7}{8}{9} for request = {10} with resourceRefCount = {11}{12,choice,0#|1# suppressed log event original at {13,date,short} {13,time,medium}|1< with {12,number,integer} identical entries suppressed between {13,date,short} {13,time,medium} and {14,date,short} {14,time,medium}}");

    @VisibleForTesting
    final Map<ResourceType, List<AclLoggingConfig>> aclLoggingMap = new EnumMap<>(ResourceType.class);

    private Cache<CacheKey, CacheEntry> loggingEventCache;
    private ImmutableSet<ApiKeys> suppressOperations;

    @Override
    public void configure(Map<String, ?> configs) {
        ConfigDef defs = new ConfigDef();
        defs.define(LOGGING_PREFIX + "suppressionWindow.duration",
                ConfigDef.Type.STRING,
                Duration.ofSeconds(1).toString(),
                ConfigDef.CompositeValidator.of(
                        new ConfigDef.NonEmptyString(),
                        (name, value) -> Duration.parse((String) value)),
                ConfigDef.Importance.LOW,
                "The duration over which repeated messages should be suppressed.");
        defs.define(LOGGING_PREFIX + "suppressionWindow.eventCount",
                ConfigDef.Type.INT,
                5000,
                ConfigDef.Range.between(0, 100000),
                ConfigDef.Importance.LOW,
                "A cap on the number of different event suppression windows to hold.");

        defs.define(LOGGING_PREFIX + "suppressionWindow.apis",
                ConfigDef.Type.STRING,
                "PRODUCE,FETCH",
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.LOW,
                "THe APIs for which we should suppress *duplicate* events");
        final AbstractConfig configParser = new AbstractConfig(defs, configs);

        configs.entrySet()
                .stream()
                .filter(config -> ACL_LOGGING_PATTERN.matcher(config.getKey()).matches())
                .map(Map.Entry::getValue)
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .map(AclLoggingConfig::valueOf)
                .flatMap(List::stream)
                .forEach(binding -> aclLoggingMap.compute(binding.getResourcePattern().resourceType(), (k, v) -> {
                    List<AclLoggingConfig> bindings = Objects.requireNonNullElseGet(v, ArrayList::new);
                    bindings.add(binding);
                    return bindings;
                }));

        configureRepeatedMessageSuppression(configParser);
    }

    @Override
    public void close() {
        if (loggingEventCache != null) {
            loggingEventCache.invalidateAll();
        }
    }

    public void logAuditMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        if ((authorized && action.logIfAllowed()) ||
                (!authorized && action.logIfDenied())) {
            final ApiKeys apiKey = ApiKeys.forId(requestContext.requestType());
            if (suppressOperations.contains(apiKey)) {
                final CacheKey cacheKey = new CacheKey(action.resourcePattern().name(), action.operation(), requestContext.principal(), requestContext.requestType(), requestContext.listenerName(), authorized);
                final CacheEntry cacheEntry;
                try {
                    cacheEntry = loggingEventCache.get(cacheKey, () -> {
                        //  Log first entry immediately to ensure the logs still read sensibly
                        logAtAllowedLevel(logLevelFor(requestContext, action),
                                () -> buildLogMessage(requestContext, action, authorized));
                        return new CacheEntry(
                                logLevelFor(requestContext, action),
                                (suppressedCount, windowStart, windowEnd) -> buildLogMessage(requestContext, action, authorized, suppressedCount, windowStart, windowEnd));

                    });
                    cacheEntry.repeated();
                } catch (ExecutionException e) {
                    log.warn("Error suppressing repeated log message, logging immediately. Due to {}", e.getMessage(), e);
                    logAtAllowedLevel(logLevelFor(requestContext, action),
                            () -> buildLogMessage(requestContext, action, authorized));
                }
            } else {
                logAtAllowedLevel(logLevelFor(requestContext, action),
                        () -> buildLogMessage(requestContext, action, authorized));
            }
        } else if (auditLogger.isTraceEnabled()) {
            auditLogger.trace(buildLogMessage(requestContext, action, authorized));
        }
    }

    public void logAtLevel(AuthorizableRequestContext requestContext, Action action, String prefox, boolean authorized) {
        logAtAllowedLevel(logLevelFor(requestContext, action), () -> prefox + buildLogMessage(requestContext, action, authorized));
    }

    Level logLevelFor(AuthorizableRequestContext requestContext, Action action) {
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

    void logAtAllowedLevel(Level lvl, Supplier<String> msg) {
        switch (lvl) {
            case ERROR:
                if (auditLogger.isErrorEnabled()) {
                    auditLogger.error(msg.get());
                }
                break;
            case WARN:
                if (auditLogger.isWarnEnabled()) {
                    auditLogger.warn(msg.get());
                }
                break;
            case INFO:
                if (auditLogger.isInfoEnabled()) {
                    auditLogger.info(msg.get());
                }
                break;
            case DEBUG:
                if (auditLogger.isDebugEnabled()) {
                    auditLogger.debug(msg.get());
                }
                break;
            case TRACE:
                if (auditLogger.isTraceEnabled()) {
                    auditLogger.trace(msg.get());
                }
                break;
        }
    }

    @VisibleForTesting
    void evictWindowedEvents() {
        loggingEventCache.invalidateAll();
    }

    private void configureRepeatedMessageSuppression(AbstractConfig configParser) {
        final int eventCount = configParser.getInt(LOGGING_PREFIX + "suppressionWindow.eventCount");
        final Duration cacheDuration = Duration.parse(configParser.getString(LOGGING_PREFIX + "suppressionWindow.duration"));
        final String apisCsv = configParser.getString(LOGGING_PREFIX + "suppressionWindow.apis");
        final Set<ApiKeys> configuredOperations = StreamSupport.stream(CSV_SPLITTER.split(apisCsv).spliterator(), false).map(ApiKeys::valueOf).collect(Collectors.toSet());

        suppressOperations = Sets.immutableEnumSet(configuredOperations);
        loggingEventCache = CacheBuilder.newBuilder()
                .maximumSize(eventCount)
                .expireAfterWrite(cacheDuration)
                .removalListener((RemovalListener<CacheKey, CacheEntry>) removalNotification ->
                        removalNotification.getValue().log())
                .build();
    }

    private String buildLogMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized) {
        return buildLogMessage(requestContext, action, authorized, 0L, null, null);
    }

    private String buildLogMessage(AuthorizableRequestContext requestContext, Action action, boolean authorized, long suppressedCount, Instant windowStart, Instant windowEnd) {
        Principal principal = requestContext.principal();
        String operation = SecurityUtils.operationName(action.operation());
        String host = requestContext.clientAddress().getHostAddress();
        String listenerName = requestContext.listenerName();
        String resourceType = SecurityUtils.resourceTypeName(action.resourcePattern().resourceType());
        String authResult = authorized ? "Allowed" : "Denied";
        Object apiKey = ApiKeys.hasId(requestContext.requestType()) ? ApiKeys.forId(requestContext.requestType()).name() : requestContext.requestType();
        int refCount = action.resourceReferenceCount();
        final java.util.Date from = windowStart != null ? Date.from(windowStart) : null;
        final java.util.Date until = windowEnd != null ? Date.from(windowEnd) : null;
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
                suppressedCount,
                from,
                until});
    }

    private static final class CacheKey {
        private final String resourceName;
        private final AclOperation aclOperation;
        private final KafkaPrincipal principal;
        private final ApiKeys apiKey;
        private final String listenerName;
        private final boolean authorized;

        private CacheKey(String resourceName, AclOperation aclOperation, KafkaPrincipal principal, int requestType, String listenerName, boolean authorized) {
            this.resourceName = resourceName;
            this.aclOperation = aclOperation;
            this.principal = principal;
            this.apiKey = ApiKeys.forId(requestType);
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
            return authorized == cacheKey.authorized && Objects.equals(resourceName, cacheKey.resourceName) && aclOperation == cacheKey.aclOperation && Objects.equals(principal, cacheKey.principal) && apiKey == cacheKey.apiKey && Objects.equals(listenerName, cacheKey.listenerName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(resourceName, aclOperation, principal, apiKey, listenerName, authorized);
        }
    }

    @FunctionalInterface
    private interface MessageGenerator {
        String generate(long messageCount, Instant windowStart, Instant windowEnd);
    }

    private final class CacheEntry {
        private final LongAdder suppressionCounter;
        private final Level logLevel;
        private final MessageGenerator messageGenerator;
        private final Instant firstEntry;
        private Instant finalEntry;

        private CacheEntry(Level logLevel, MessageGenerator messageGenerator) {
            this.logLevel = logLevel;
            this.messageGenerator = messageGenerator;
            suppressionCounter = new LongAdder();
            firstEntry = Instant.now();
            finalEntry = Instant.now();
        }

        public void repeated() {
            suppressionCounter.increment();
            finalEntry = Instant.now();
        }

        public void log() {
            final long messageCount = suppressionCounter.sumThenReset();
            if (messageCount > 1) {
                //Given we logged the event which created the window there are N-1 events covered by the window
                logAtAllowedLevel(logLevel, () -> messageGenerator.generate(messageCount - 1, firstEntry, finalEntry));
            }
        }
    }
}
