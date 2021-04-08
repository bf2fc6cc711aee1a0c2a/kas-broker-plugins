/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static io.bf2.kafka.authorizer.GlobalAclAuthorizer.CONFIG_PREFIX;
import static io.bf2.kafka.authorizer.GlobalAclAuthorizer.ALLOWED_LISTENERS;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;

public class GlobalAclAuthorizerTest
{
    @Test
    public void testLoad(){
        GlobalAclAuthorizer auth = new GlobalAclAuthorizer();

        HashMap<String, Object> config = new HashMap<>();
        config.put( CONFIG_PREFIX + ALLOWED_LISTENERS, "canary, loop");
        config.put(CONFIG_PREFIX + "acl.1", "permission=allow;topic=foo;operations=read,write,create");
        config.put(CONFIG_PREFIX + "acl.2", "permission=allow;topic=bar;operations=read");
        config.put(CONFIG_PREFIX + "acl.3", "permission=deny;group=xyz;operations=read,create");

        auth.configure(config);

        AclBinding acl = auth.aclMap.get(ResourceType.TOPIC).get(0);
        assertTrue(acl.entry().permissionType() == AclPermissionType.ALLOW);
        ResourcePattern resource = acl.pattern();
        assertEquals("foo", resource.name());
        assertEquals(ResourceType.TOPIC, resource.resourceType());
        assertEquals(AclOperation.READ, acl.entry().operation());

        acl = auth.aclMap.get(ResourceType.TOPIC).get(1);
        assertTrue(acl.entry().permissionType() == AclPermissionType.ALLOW);
        resource = acl.pattern();
        assertEquals("foo", resource.name());
        assertEquals(ResourceType.TOPIC, resource.resourceType());
        assertEquals(AclOperation.WRITE, acl.entry().operation());

        acl = auth.aclMap.get(ResourceType.TOPIC).get(2);
        assertTrue(acl.entry().permissionType() == AclPermissionType.ALLOW);
        resource = acl.pattern();
        assertEquals("foo", resource.name());
        assertEquals(ResourceType.TOPIC, resource.resourceType());
        assertEquals(AclOperation.CREATE, acl.entry().operation());

        acl = auth.aclMap.get(ResourceType.TOPIC).get(3);
        assertTrue(acl.entry().permissionType() == AclPermissionType.ALLOW);
        resource = acl.pattern();
        assertEquals("bar", resource.name());
        assertEquals(ResourceType.TOPIC, resource.resourceType());
        assertEquals(AclOperation.READ, acl.entry().operation());

        acl = auth.aclMap.get(ResourceType.GROUP).get(0);
        assertTrue(acl.entry().permissionType() == AclPermissionType.DENY);
        resource = acl.pattern();
        assertEquals("xyz", resource.name());
        assertEquals(ResourceType.GROUP, resource.resourceType());
        assertEquals(AclOperation.READ, acl.entry().operation());

        acl = auth.aclMap.get(ResourceType.GROUP).get(1);
        assertTrue(acl.entry().permissionType() == AclPermissionType.DENY);
        resource = acl.pattern();
        assertEquals("xyz", resource.name());
        assertEquals(ResourceType.GROUP, resource.resourceType());
        assertEquals(AclOperation.CREATE, acl.entry().operation());

        assertEquals(auth.allowedListeners, Arrays.asList("canary", "loop"));

        auth.close();
    }

    @Test
    public void testAuthorize() {
        GlobalAclAuthorizer auth = new GlobalAclAuthorizer() {
            @Override
            public boolean isSuperUser(KafkaPrincipal principal) {
                if (principal.getName().equals("admin")) {
                    return true;
                }
                return false;
            }
        };

        HashMap<String, Object> config = new HashMap<>();
        config.put(CONFIG_PREFIX + "acl.1", "permission=allow;topic=foo;operations=read,write,create");
        config.put(CONFIG_PREFIX + "acl.2", "permission=allow;topic=bar;operations=read");
        config.put(CONFIG_PREFIX + "acl.3", "permission=deny;group=xyz;operations=read,create");
        config.put(CONFIG_PREFIX + "acl.4", "permission=allow;topic=abc;operations=all");
        config.put(CONFIG_PREFIX + "acl.5", "permission=allow;topic=*;operations=read");
        config.put(CONFIG_PREFIX + ALLOWED_LISTENERS, " canary, loop");


        auth.configure(config);

        Action action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL), 0, false, false);
        AuthorizableRequestContext rc = Mockito.mock(AuthorizableRequestContext.class);
        Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal("user", "any"));
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        action = new Action(AclOperation.DELETE, new ResourcePattern(ResourceType.TOPIC, "foo", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.DENIED), auth.authorize(rc, Arrays.asList(action)));

        // "*" matched topic
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        action = new Action(AclOperation.WRITE, new ResourcePattern(ResourceType.TOPIC, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.DENIED), auth.authorize(rc, Arrays.asList(action)));

        // match any
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.TOPIC, "abc", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        action = new Action(AclOperation.WRITE, new ResourcePattern(ResourceType.TOPIC, "abc", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        // group check
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.DENIED), auth.authorize(rc, Arrays.asList(action)));

        // deny cluster
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.CLUSTER, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.DENIED), auth.authorize(rc, Arrays.asList(action)));

        // super user allow
        Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal("user", "admin"));
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        Mockito.when(rc.principal()).thenReturn(new KafkaPrincipal("user", "noadmin"));
        Mockito.when(rc.listenerName()).thenReturn("loop");
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        Mockito.when(rc.listenerName()).thenReturn("loop-9021://127.0.0.1:9021");
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.ALLOWED), auth.authorize(rc, Arrays.asList(action)));

        Mockito.when(rc.listenerName()).thenReturn("something");
        action = new Action(AclOperation.READ, new ResourcePattern(ResourceType.GROUP, "xyz", PatternType.LITERAL), 0, false, false);
        assertEquals(Arrays.asList(AuthorizationResult.DENIED), auth.authorize(rc, Arrays.asList(action)));

        auth.close();
    }
}
