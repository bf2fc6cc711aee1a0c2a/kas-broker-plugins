/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * A custom authorizer for Kafka that defines ACLs for all the users. The configuration is provided as
 * <pre>
 * acl.1: permission=allow;topic=foo;operations=read,write,create
 * acl.2: permission=allow;topic=bar;operations=read
 * acl.3: permission=deny;group=xyz;operations=read,create
 * </pre>
 * supported resource types are "topic, group, cluster, transaction_id, deletegation_token"
 * supported permissions are "allow" and "deny"
 * supported operations are "all,read,write,delete,create,describe,alter,cluster_action,describe_configs,alter_configs"
 */
public class GlobalAclAuthorizer extends kafka.security.authorizer.AclAuthorizer {

    private static final String OPERATIONS = "operations";
    private static final String PERMISSION = "permission";
    static final String ALLOWED_LISTENERS = "allowed-listeners";
    static final String CONFIG_PREFIX = "strimzi.authorization.global-authorizer.";

    static final Logger log = LoggerFactory.getLogger(GlobalAclAuthorizer.class);

    Map<ResourceType, List<PattenMatchedAclBinding>> aclMap = new HashMap<>();

    List<UserSpec> superUsers = Collections.emptyList();

    Cache<CacheKey, Boolean> cache = CacheBuilder.newBuilder()
            .maximumSize(50000)
            .initialCapacity(5000)
            .expireAfterWrite(Duration.ofHours(1L))
            .weakKeys()
            .build();

    List<String> allowedListeners = Collections.emptyList();

    @Override
    public void configure(Map<String, ?> configs) {

        // read all super users, these users are always granted allow
        String users = (String) configs.get("super.users");
        if (users != null) {
            this.superUsers = Arrays.asList(users.split(";"))
                    .stream()
                    .map(s -> UserSpec.of(s))
                    .collect(Collectors.toList());
        }

        // allowed listeners, any request made on this listener is by default granted to allow
        String listeners = (String)configs.get(CONFIG_PREFIX + ALLOWED_LISTENERS);
        if (listeners != null && !listeners.isEmpty()) {
            this.allowedListeners = Arrays.asList(listeners.split("\\s*,\\s*"));
        }

        log.info("Allowed Custom Group Authorizer Listeners {}", this.allowedListeners);

        // read custom ACLs configured for rest of the users
        int i = 1;
        while(true) {
            String key = CONFIG_PREFIX + "acl." + i++;
            String value = (String)configs.get(key);

            if (value == null) {
                break;
            }

            ResourcePattern resourcePattern = null;
            AclPermissionType permissionType = AclPermissionType.ALLOW;
            List<AclOperation> operations = new ArrayList<>();

            StringTokenizer st = new StringTokenizer(value, ";");
            while(st.hasMoreTokens()) {
                String token = st.nextToken();
                int idx = token.indexOf('=');
                String k = token.substring(0, idx).toLowerCase();
                String v = token.substring(idx+1).trim();

                if (k.equals(OPERATIONS)) {
                    String[] ops = v.split("\\s*,\\s*");
                    for (String str:ops) {
                        AclOperation op = AclOperation.fromString(str);
                        if (op == AclOperation.UNKNOWN || op == AclOperation.ANY) {
                            throw new IllegalArgumentException("Operation Type " + k + " is invalid or not supported");
                        }
                        operations.add(op);
                    }
                } else if (k.equals(PERMISSION)) {
                    permissionType = AclPermissionType.fromString(v);
                    if (permissionType == AclPermissionType.UNKNOWN || permissionType == AclPermissionType.ANY) {
                        throw new IllegalArgumentException("Permission Type " + k + " is invalid or not supported");
                    }
                } else {
                    ResourceType type = ResourceType.fromString(k);
                    if (type == ResourceType.UNKNOWN) {
                        throw new IllegalArgumentException("Resource type " + k + " is invalid or not supported");
                    }
                    resourcePattern = new ResourcePattern(type, v, PatternType.PREFIXED);
                }
            }

            for (AclOperation op:operations ) {
                PattenMatchedAclBinding binding = new PattenMatchedAclBinding(resourcePattern, new AccessControlEntry("all", "*", op, permissionType));
                List<PattenMatchedAclBinding> acls = this.aclMap.get(resourcePattern.resourceType());
                if (acls == null) {
                    acls = new ArrayList<>();
                    this.aclMap.put(resourcePattern.resourceType(), acls);
                }
                acls.add(binding);
            };
        }
        log.info("Custom Authorizer ACLs configured {}", this.aclMap);
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        ArrayList<AuthorizationResult> results = new ArrayList<>(actions.size());
        actions.forEach(action -> {
            boolean authorized = authorizeAction(requestContext, action);
            if (authorized) {
                results.add(AuthorizationResult.ALLOWED);
                if (action.logIfAllowed()) {
                    log.debug("user {} allowed for operation {} on resource {} using listener {}",
                            requestContext.principal().getName(),
                            action.operation(),
                            action.resourcePattern().name(),
                            requestContext.listenerName());
                }
            } else {
                results.add(AuthorizationResult.DENIED);
                if (action.logIfDenied()) {
                    log.debug("user {} denied for operation {} on resource {} using listener {}",
                            requestContext.principal().getName(),
                            action.operation(),
                            action.resourcePattern().name(),
                            requestContext.listenerName());
                }
            }
        });
        return results;
    }

    private class CacheKey {
        String name;
        AclOperation operation;

        public CacheKey(Action action){
            this.name = action.resourcePattern().name();
            this.operation = action.operation();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            result = prime * result + ((operation == null) ? 0 : operation.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            CacheKey other = (CacheKey) obj;
            if(!name.equals(other.name)) {
                return false;
            }
            if (operation != other.operation) {
                return false;
            }
            return true;
        }
    }

    private boolean authorizeAction(AuthorizableRequestContext requestContext, Action action) {

        // is super user allow any operation
        if (isSuperUser(requestContext.principal())) {
            if(log.isDebugEnabled()) {
                log.debug("super.user {} allowed for operation {} on resource {} using listener {}",
                        requestContext.principal().getName(),
                        action.operation(),
                        action.resourcePattern().name(),
                        requestContext.listenerName());
            }
            return true;
        }

        // if request made on any allowed listeners allow always
        if (isAllowedListener(requestContext.listenerName())) {
            if(log.isDebugEnabled()) {
                log.debug("listener {} allowed for operation {} on resource {} using listener {}",
                        requestContext.listenerName(),
                        action.operation(),
                        action.resourcePattern().name(),
                        requestContext.listenerName());
            }
            return true;
        }

        // for all other uses lets check the permission
        if(log.isDebugEnabled()) {
            log.debug("User {} asking for permission {} using listener {}",
                    requestContext.principal().getName(),
                    action.operation(),
                    requestContext.listenerName());
        }

        // super user actions are not cached.
        CacheKey cachekey = new CacheKey(action);
        ResourceType kafkaResourceType = action.resourcePattern().resourceType();
        Boolean result = this.cache.getIfPresent(cachekey);
        if (result == null) {
            result = Boolean.valueOf(allow(action, this.aclMap.get(kafkaResourceType)));
            this.cache.put(cachekey, result);
        }
        return result;
    }

    private boolean allow(Action action, List<PattenMatchedAclBinding> acls) {
        if (acls == null || acls.isEmpty()) {
            return false;
        }
        for (PattenMatchedAclBinding binding:acls) {
            Boolean result = allow(action, binding);
            if (result != null) {
                return result;
            }
        }
        return false;
    }

    private Boolean allow(Action action, PattenMatchedAclBinding binding) {
        if (binding.matches(action.resourcePattern().name())) {
            if (binding.entry().operation() == AclOperation.ALL || binding.entry().operation() == action.operation()) {
                if (binding.entry().permissionType() == AclPermissionType.ALLOW) {
                    return true;
                } else {
                    return false;
                }
            }
        }
        return null;
    }

    @Override
    public boolean isSuperUser(KafkaPrincipal principal) {
        for (UserSpec u : this.superUsers) {
            if (principal.getPrincipalType().equals(u.getType())
                    && principal.getName().equals(u.getName())) {
                return true;
            }
        }
        return false;
    }

    // openshift format PLAIN-9092://0.0.0.0:9092,OPEN-9093://0.0.0.0:9093,SRE-9096://0.0.0.0:9096
    // minikube PLAIN-9092,OPEN-9093,SRE-9096
    public boolean isAllowedListener(String listener) {
        if (listener != null) {
            for (String str : this.allowedListeners) {
                if (listener.startsWith(str)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext,
            List<AclBinding> aclBindings) {
        return super.createAcls(requestContext, aclBindings);
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext,
            List<AclBindingFilter> aclBindingFilters) {
        return super.deleteAcls(requestContext, aclBindingFilters);
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return super.acls(filter);
    }
}
