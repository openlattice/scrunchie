package com.kryptnostic.sparks;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.SearchConfiguration;
import com.kryptnostic.kindling.search.ConductorElasticsearchImpl;

import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic;
import pl.allegro.tech.embeddedelasticsearch.PopularProperties;

import org.elasticsearch.common.settings.Settings;

public class KindlingElasticsearchTests extends BaseKindlingSparkTest {
	
    private static final UUID   ENTITY_SET_ID  = UUID.fromString( "0a648f39-5e41-46b5-a928-ec44cdeeae13" );
    private static final UUID   ENTITY_TYPE_ID = UUID.fromString( "c271a300-ea05-420b-b33b-8ecb18de5ce7" );
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
    
    private SearchConfiguration config;
    private ConductorElasticsearchImpl elasticsearchApi;
    private final Logger logger = LoggerFactory.getLogger( getClass() );
    
    @BeforeClass
    public static void startElasticsearchCluster() {
    	Settings settings = Settings.builder().put( "cluster.name", "loom_development" ).build();
    	try {
			EmbeddedElastic elastic = EmbeddedElastic.builder()
					.withElasticVersion( "5.1.1" )
					.withSetting( PopularProperties.TRANSPORT_TCP_PORT, 9300 )
					.withSetting( PopularProperties.CLUSTER_NAME, "loom_development" )
					.build()
					.start();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
    }

    @Before
    public void init() {
    	this.config = new SearchConfiguration( "localhost", "loom_development", 9300 );
    	try {
			this.elasticsearchApi = new ConductorElasticsearchImpl( config );
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
    }
        
    @Test
    public void initializeIndex() {
		elasticsearchApi.initializeEntitySetDataModelIndex();
    }

    @Test
    public void testWriteEntitySetMetadata() {
    	EntitySet entitySet = new EntitySet(
    			ENTITY_SET_ID,
    			ENTITY_TYPE,
    			ENTITY_TYPE_ID,
    			ENTITY_SET_NAME,
    			TITLE,
    			Optional.of(DESCRIPTION) );
        PropertyType empId = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
        		"Employee Id",
        		Optional.of("id of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empName = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
        		"Employee Name",
        		Optional.of("name of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empTitle = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
        		"Employee Title",
        		Optional.of("title of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empSalary = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, SALARY ),
        		"Employee Salary",
        		Optional.of("salary of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empDept = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
        		"Employee Department",
        		Optional.of("department of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
    	

        List<PropertyType> propertyTypes = Lists.newArrayList();
        propertyTypes.add( empId );
        propertyTypes.add( empName );
        propertyTypes.add( empTitle );
        propertyTypes.add( empSalary );
        propertyTypes.add( empDept );
        
        Principal owner = new Principal( PrincipalType.USER, "support@kryptnostic.com" );
        
		elasticsearchApi.saveEntitySetToElasticsearch( entitySet, propertyTypes, owner );
    }
    
    @Test
    public void testAddEntitySetPermissions() {
    	Principal principal = new Principal( PrincipalType.ROLE, "user" );
    	Set<Permission> newPermissions = Sets.newHashSet();
    	newPermissions.add( Permission.WRITE );
    	newPermissions.add( Permission.READ );
		elasticsearchApi.updateEntitySetPermissions( ENTITY_SET_ID, principal, newPermissions );
    }
    
    
    @Test
    public void testEntitySetKeywordSearch() {
    	Set<Principal> principals = Sets.newHashSet();
    //	principals.add( new Principal( PrincipalType.USER, "katherine" ) );
    	//principals.add( new Principal( PrincipalType.ROLE, "evil" ) );
    	principals.add( new Principal( PrincipalType.ROLE, "user" ) );
    	
    	String query = "Employees";
    	FullQualifiedName entityTypeFqn = new FullQualifiedName("testcsv", "employee");
    	List<FullQualifiedName> propertyTypes = Lists.newArrayList();
    	propertyTypes.add( new FullQualifiedName( "testcsv", "salary") );
    	propertyTypes.add( new FullQualifiedName( "testcsv", "employee_dept" ) );
		elasticsearchApi.executeEntitySetDataModelKeywordSearch(
				Optional.of( query ),
				Optional.of( UUID.fromString("b87afe10-5963-4222-bf42-b39eec398744") ),
				Optional.of( Sets.newHashSet() ),
				principals
//				Optional.of( entityTypeFqn ),
		//		Optional.of( propertyTypes )
		);
    }
    
    @Test
    public void testUpdatePropertyTypes() {
    	PropertyType empId = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_ID ),
        		"Employee Id",
        		Optional.of("id of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empName = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_NAME ),
        		"Employee Name",
        		Optional.of("name of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empTitle = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_TITLE ),
        		"Employee Title",
        		Optional.of("title of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empSalary = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, SALARY ),
        		"Employee Salary",
        		Optional.of("salary of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
        PropertyType empDept = new PropertyType(
        		UUID.randomUUID(),
        		new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ),
        		"Employee Department",
        		Optional.of("department of the employee"),
        		Sets.newHashSet(),
        		EdmPrimitiveTypeKind.String );
    	

        Set<PropertyType> propertyTypes = Sets.newHashSet();
        propertyTypes.add( empId );
        propertyTypes.add( empName );
        propertyTypes.add( empTitle );
        propertyTypes.add( empSalary );
        propertyTypes.add( empDept );
		elasticsearchApi.updatePropertyTypesInEntitySet( ENTITY_SET_ID, propertyTypes);
    }
    
    @Test
    public void testDeleteEntitySet() {
		elasticsearchApi.deleteEntitySet( ENTITY_SET_ID );
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
