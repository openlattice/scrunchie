package com.kryptnostic.sparks;

import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.mapstores.v2.Permission;

public class SparkAuthorizationManager {
    Set<UUID> getAuthorizedAcls( UUID userId , Permission permission ) {
        //TODO: Implement this
        return ImmutableSet.of( ACLs.EVERYONE_ACL );
    }
}
