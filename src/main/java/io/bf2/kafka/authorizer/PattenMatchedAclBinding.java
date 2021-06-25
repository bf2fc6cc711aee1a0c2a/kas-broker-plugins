/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.regex.Pattern;

class PattenMatchedAclBinding extends AclBinding {

    private Pattern pattern;

    PattenMatchedAclBinding(ResourcePattern resource, AccessControlEntry entry) {
        super(resource, entry);
        String str = resource.name();
        if (str.equals("*")) {
            str = ".*";
        }
        // try to pattern with meta chars
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '*' && str.charAt(i-1) != '.') {
                sb.append('.').append(c);
            } else {
                sb.append(c);
            }
        }
        this.pattern = Pattern.compile(str);
    }

    public boolean matches(String str) {
        return pattern.matcher(str).matches();
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
