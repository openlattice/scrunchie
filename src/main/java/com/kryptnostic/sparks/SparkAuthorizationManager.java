package com.kryptnostic.sparks;

import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.mapstores.v2.Permission;

public class SparkAuthorizationManager {
    List<UUID> getAuthorizedAcls( UUID userId , Permission permission ) {
        //TODO: Implement this
        return ImmutableList.of( ACLs.EVERYONE_ACL );
    }
}
