package com.kryptnostic.sparks;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.kryptnostic.kindling.search.KindlingConfiguration;
import com.kryptnostic.kindling.search.KindlingElasticsearchHandler;

public class KindlingElasticsearchTests {
	
    private static final UUID   ENTITY_SET_ID  = UUID.fromString( "0a648f39-5e41-46b5-a928-ec44cdeeae13" );
    private static final String TITLE          = "The Entity Set Title";
    private static final String DESCRIPTION    = "This is a description for the entity set called employees.";
    
    private static final String NAMESPACE                = "testcsv";
    private static final String SALARY                   = "salary";
    private static final String EMPLOYEE_NAME            = "employee_name";
    private static final String EMPLOYEE_TITLE           = "employee_title";
    private static final String EMPLOYEE_DEPT            = "employee_dept";
    private static final String EMPLOYEE_ID              = "employee_id";
    private static final String WEIGHT					 = "weight";
    private static final String ENTITY_SET_NAME          = "Employees";
    private static final FullQualifiedName    ENTITY_TYPE              = new FullQualifiedName(
            NAMESPACE,
            "employee" );
    
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    
    @Test
    public void initializeIndex() {
    	KindlingConfiguration config = new KindlingConfiguration( Optional.of("localhost"), Optional.of("loom_development") );
		KindlingElasticsearchHandler keh;
		try {
			keh = new KindlingElasticsearchHandler( config );
			keh.initializeEntitySetDataModelIndex();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
    

    @Test
    public void testWriteEntitySetMetadata() {
    	EntitySet entitySet = new EntitySet(
    			ENTITY_SET_ID,
    			ENTITY_TYPE,
    			ENTITY_SET_NAME,
    			TITLE,
    			DESCRIPTION );
        PropertyType empId = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empName = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empTitle = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empSalary = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, SALARY ),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empDept = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
    	

        Set<PropertyType> propertyTypes = Sets.newHashSet();
        propertyTypes.add( empId );
        propertyTypes.add( empName );
        propertyTypes.add( empTitle );
        propertyTypes.add( empSalary );
        propertyTypes.add( empDept );
        
		KindlingConfiguration config = new KindlingConfiguration( Optional.of("localhost"), Optional.of("loom_development") );
		KindlingElasticsearchHandler keh;
		try {
			keh = new KindlingElasticsearchHandler( config );
			keh.saveEntitySetToElasticsearch( entitySet, propertyTypes );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
    
    @Test
    public void testEntitySetKeywordSearch() {
    	String userId = "kathesdfrine";
    	List<String> roles = Lists.newArrayList( "evil" );
    	//roles.add( "user");
    	
    	String query = "Employees";
    	FullQualifiedName entityTypeFqn = new FullQualifiedName("testcsv", "employee");
    	List<FullQualifiedName> propertyTypes = Lists.newArrayList();
    	propertyTypes.add( new FullQualifiedName( "testcsv", "salary") );
    	propertyTypes.add( new FullQualifiedName( "testcsv", "employee_dept" ) );
    	KindlingConfiguration config = new KindlingConfiguration( Optional.of("localhost"), Optional.of("loom_development") );
		KindlingElasticsearchHandler keh;
		try {
			keh = new KindlingElasticsearchHandler( config );
			keh.executeEntitySetDataModelKeywordSearch(
					userId,
					roles,
					query,
					Optional.absent(),
					Optional.absent()
	//				Optional.of( entityTypeFqn ),
		//			Optional.of( propertyTypes )
			);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
    
    @Test
    public void testAddEntitySetPermissions() {
    	Principal principal = new Principal( PrincipalType.ROLE, "evil" );
    	Set<Permission> newPermissions = Sets.newHashSet();
    //	newPermissions.add( Permission.READ );
    	KindlingConfiguration config = new KindlingConfiguration( Optional.of("localhost"), Optional.of("loom_development") );
		KindlingElasticsearchHandler keh;
		try {
			keh = new KindlingElasticsearchHandler( config );
			keh.updateEntitySetPermissions( ENTITY_SET_ID, principal, newPermissions );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
    }
//    
//    @Test
//    public void testWriteEntitySetData (EntitySet entitySet ) {
//    	String entityTypeUnderscore = entitySet.getType().getName() + "_" + entitySet.getType().getNamespace();
//    	Dataset<Row> entityDf = sparkSession
//    			.read()
//    			.format( "org.apache.spark.sql.cassandra" )
//    			.option( "table", entitySet.getName() )
//    			.option( "keyspace", "sparks" )
//    			.load();
//    	JavaEsSparkSQL.saveToEs( entityDf, "entity_set_data/" + entityTypeUnderscore );
//        
//        
//    	
//    }
    
    
    
}
