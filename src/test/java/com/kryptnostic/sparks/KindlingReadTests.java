package com.kryptnostic.sparks;

import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import org.apache.olingo.commons.api.data.Entity;
import org.apache.olingo.commons.api.data.Property;
import org.apache.olingo.commons.api.data.ValueType;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.conductor.rpc.UUIDs.Syncs;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EntityStorageClient;

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
        Property empDept = new Property();

        empId.setName( EMPLOYEE_ID );
        empId.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ).getFullQualifiedNameAsString() );
        empId.setValue( ValueType.PRIMITIVE, EMP_ID );

        empName.setName( EMPLOYEE_NAME );
        empName.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ).getFullQualifiedNameAsString() );
        empName.setValue( ValueType.PRIMITIVE, "Tom" );

        empTitle.setName( EMPLOYEE_TITLE );
        empTitle.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empTitle.setValue( ValueType.PRIMITIVE, "Major" );

        empDept.setName( EMPLOYEE_DEPT );
        empDept.setType( new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ).getFullQualifiedNameAsString() );
        empDept.setValue( ValueType.PRIMITIVE, "Police" );

        empSalary.setName( SALARY );
        empSalary.setType( new FullQualifiedName( NAMESPACE, SALARY ).getFullQualifiedNameAsString() );
        empSalary.setValue( ValueType.PRIMITIVE, Long.MAX_VALUE );

        Entity e = new Entity();
        e.setType( ENTITY_TYPE.getFullQualifiedNameAsString() );
        e.addProperty( empId ).addProperty( empName ).addProperty( empTitle ).addProperty( empDept )
                .addProperty( empSalary );
        OBJECT_ID = esc.createEntityData( ACLs.EVERYONE_ACL,
                Syncs.BASE.getSyncId(),
                ENTITY_SET_NAME,
                ENTITY_TYPE,
                e ).getKey();
    }

    @Test
    public void testStepOutOfTheCapsule() {
        UUID userId = UUID.randomUUID();
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        String typename = ctb.getTablenameForPropertyIndexOfType( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) );
        LookupEntitiesRequest request = new LookupEntitiesRequest(
                userId,
                ImmutableSet.of( ENTITY_TYPE ),
                ImmutableMap.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ), EMP_ID ) );
        csi.filterEntities( request );

        csi.loadAllEntitiesOfType( ENTITY_TYPE );
    }

    @Test
    public void testGroundControlToMajorTom() {
        UUID userId = UUID.randomUUID();
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        LookupEntitiesRequest request = new LookupEntitiesRequest(
                userId,
                ImmutableSet.of( ENTITY_TYPE ),
                ImmutableMap.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ), EMP_ID ) );
        List<UUID> ids = csi.lookupEntities( request );

        Assert.assertTrue( ids.contains( OBJECT_ID ) );
    }

    @Test
    public void takeYourProteinPillsMajorTom() {
        JavaRDD<String> s = javaContext.textFile( "src/test/resources/employees.csv" );
        s.foreach( l -> System.out.println( l ) );
        JavaRDD<Employee> t = s.map( e -> Employee.EmployeeCsvReader.getEmployee( e ) );
        SQLContext context = new SQLContext( spark );
        logger.info( "Total # of employees: {}", t.count() );
        DataFrame df = context.createDataFrame( t, Employee.class );
        df.registerTempTable( "employees" );
        DataFrame emps = context.sql( "SELECT * from employees WHERE salary > 81500" );
        List<String> highlyPaidEmps = emps.javaRDD().map( e -> String.format( "%s,%s,%s,%d",
                e.getAs( "name" ),
                e.getAs( "dept" ),
                e.getAs( "title" ),
                e.getAs( "salary" ) ) ).collect();
        highlyPaidEmps.forEach( e -> System.out.println( e ) );

        logger.info( "emps: {}", Lists.newArrayList( emps.javaRDD().map( e -> new Employee(
                e.getAs( "name" ),
                e.getAs( "dept" ),
                e.getAs( "title" ),
                (int) e.getAs( "salary" ) ) ).collect() ) );
    }

    @Test
    public void testGetTableName() {
        CassandraTableManager cassandraTableManager = ds.getContext().getBean( CassandraTableManager.class );

        // Get table name for entity type
        // 1. Get table name for entity type by using Fqn
        String entityTableName = cassandraTableManager.getTablenameForEntityType( ENTITY_TYPE );
        logger.info( entityTableName );
        // 2. Get table name for entity type by using EntityType
        EntityType entityType = new EntityType().setNamespace( NAMESPACE ).setName( ENTITY_TYPE.getName() )
                .setKey( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ) ) )
                .setProperties( ImmutableSet.of( new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
                        new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
                        new FullQualifiedName( NAMESPACE, SALARY ) ) );
        String entityTableName2 = cassandraTableManager.getTablenameForEntityType( entityType );
        logger.info( entityTableName2 );

        Assert.assertEquals( entityTableName, entityTableName2 );

        // Get table name for property values
        // 1. using Fqn
        String propertyTableName = cassandraTableManager
                .getTablenameForPropertyValuesOfType( new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ) );
        logger.info( propertyTableName );
        // 2. using PropertyType
        String propertyTableName2 = cassandraTableManager
                .getTablenameForPropertyValuesOfType( new PropertyType().setNamespace( NAMESPACE )
                        .setName( EMPLOYEE_NAME )
                        .setDatatype( EdmPrimitiveTypeKind.String ).setMultiplicity( 0 ) );
        logger.info( propertyTableName2 );

        Assert.assertEquals( propertyTableName, propertyTableName2 );
    }

//    @Test
//    public void testInitializeCacheTable() {
//        String cacheTableName = csi.initializeTempTable( Sets.newHashSet(
//                new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
//                new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
//                new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
//                new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
//                new FullQualifiedName( NAMESPACE, SALARY ) ) );
//        logger.info( cacheTableName );
//    }

    @Test
    public void testWrites() {
        csi.loadAllEntitiesOfType( ENTITY_TYPE );
    }
}
