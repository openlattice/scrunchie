package com.kryptnostic.sparks;

import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.kryptnostic.conductor.rpc.LoadEntitiesRequest;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.types.services.CassandraTableManager;
import com.kryptnostic.types.services.EntityStorageClient;

public class KindlingReadTests extends BaseKindlingSparkTest {
    private static UUID OBJECT_ID;
    private static UUID EMP_ID = UUID.randomUUID();
    @BeforeClass
    public static void initData() {
        EntityStorageClient esc = ds.getContext().getBean( EntityStorageClient.class );
        Property empId = new Property();
        Property empName = new Property();
        Property empTitle = new Property();
        Property empSalary = new Property();
        empId.setName( EMPLOYEE_ID );
        empId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
        empId.setValue( ValueType.PRIMITIVE, EMP_ID );

        empName.setName( EMPLOYEE_NAME );
        empName.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
        empName.setValue( ValueType.PRIMITIVE, "Tom" );

        empTitle.setName( EMPLOYEE_TITLE );
        empTitle.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empTitle.setValue( ValueType.PRIMITIVE, "Major" );

        empSalary.setName( SALARY );
        empSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
        empSalary.setValue( ValueType.PRIMITIVE, Long.MAX_VALUE );

        Entity e = new Entity();
        e.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
        e.addProperty( empId ).addProperty( empName ).addProperty( empTitle ).addProperty( empSalary );
        OBJECT_ID = esc.createEntityData( ACLs.EVERYONE_ACL,
                Syncs.BASE.getSyncId(),
                ENTITY_SET_NAME,
                ENTITY_TYPE,
                e ).getKey();
    }

    @Test
    public void testGroundControlToMajorTom() {
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        String typename = ctb.getTablenameForPropertyIndex( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) );
        LoadEntitiesRequest request = new LoadEntitiesRequest(
                UUID.randomUUID(),
                ImmutableMap.of( typename, EMP_ID ) );
        List<UUID> ids = csi.lookupEntities( DatastoreConstants.KEYSPACE, request );

        Assert.assertTrue( ids.contains( OBJECT_ID ) );
    }
}
