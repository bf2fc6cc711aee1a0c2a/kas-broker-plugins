package io.bf2.kafka.authorizer;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * This class supports ACL rules that apply to multiple operations (inclusive or
 * exclusive) or that apply to multiple API request types (inclusive or exclusive).
 *
 * Not intended to be passed to classes/methods outside of this package.
 */
class ApiAwareAccessControlEntry extends AccessControlEntry {

    static final String TO_STRING_ANY = "<any>";

    final Set<AclOperation> operations;
    final boolean operationsExcluded;

    final Set<ApiKeys> apiKeys;
    final boolean apiKeysExcluded;

    ApiAwareAccessControlEntry(String principal, String host,
            Set<AclOperation> operations,
            boolean operationsExcluded,
            Set<ApiKeys> apiKeys,
            boolean apiKeysExcluded,
            AclPermissionType permissionType) {

        super(principal, host, AclOperation.UNKNOWN, permissionType);

        if (operations.isEmpty()) {
            throw new IllegalArgumentException("operations must not be empty");
        }

        if (operations.contains(AclOperation.ANY) || operations.contains(AclOperation.UNKNOWN)) {
            throw new IllegalArgumentException("operations must not contain ANY or UNKNOWN");
        }

        if (permissionType == AclPermissionType.ANY || permissionType == AclPermissionType.UNKNOWN) {
            throw new IllegalArgumentException("permissionType must not be ANY or UNKNOWN");
        }

        this.operations = Collections.unmodifiableSet(EnumSet.copyOf(operations));
        this.operationsExcluded = operationsExcluded;

        Objects.requireNonNull(apiKeys);
        this.apiKeys = apiKeys.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(EnumSet.copyOf(apiKeys));
        this.apiKeysExcluded = apiKeysExcluded;
    }

    @Override
    public AclOperation operation() {
        if(!operationsExcluded && operations.size() == 1){
            return operations.iterator().next();
        } else {
            return AclOperation.UNKNOWN;
        }
    }

    public Set<AclOperation> operations() {
        return operations;
    }

    public boolean operationsExcluded() {
        return operationsExcluded;
    }

    public Set<ApiKeys> apiKeys() {
        return apiKeys;
    }

    public boolean apiKeysExcluded() {
        return apiKeysExcluded;
    }

    @Override
    public String toString() {
        return String.format("(principal=%s, host=%s, operations=%s, apiKeys=%s, permissionType=%s)",
                principal() == null ? TO_STRING_ANY : principal(),
                host() == null ? TO_STRING_ANY : host(),
                (operationsExcluded ? "exclude/" : "") + operations,
                (apiKeysExcluded ? "exclude/" : "") + (apiKeys.isEmpty() ? TO_STRING_ANY : apiKeys),
                permissionType());
    }

    @Override
    public boolean isUnknown() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ApiAwareAccessControlEntry)) {
            return false;
        }
        ApiAwareAccessControlEntry other = (ApiAwareAccessControlEntry) o;
        return super.equals(other)
                && operationsExcluded == other.operationsExcluded
                && operations.equals(other.operations)
                && apiKeysExcluded == other.apiKeysExcluded
                && apiKeys.equals(other.apiKeys);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(new int[] {
            super.hashCode(),
            operations.hashCode(),
            Boolean.hashCode(operationsExcluded),
            apiKeys.hashCode(),
            Boolean.hashCode(apiKeysExcluded)
        });
    }

}
