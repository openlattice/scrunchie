package com.kryptnostic.sparks;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.Test;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import jersey.repackaged.com.google.common.collect.Maps;

public class KindlingElasticsearchTests extends BaseElasticsearchTest {
        
    @Test
    public void initializeIndex() {
		elasticsearchApi.initializeEntitySetDataModelIndex();
    }

    @Test
    public void testWriteEntitySetMetadata() {
    	EntitySet entitySet = new EntitySet(
    			ENTITY_SET_ID,
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
    
    @Test
    public void testSearchAcrossIndices() {
        Set<UUID> entitySetIds = Sets.newHashSet();
        entitySetIds.add( UUID.fromString( "d93061ba-334a-46dd-bef0-070de71c8cf0" ) );
        Map<UUID, String> fieldSearches = Maps.newHashMap();
        fieldSearches.put( UUID.fromString( "12926a46-7b2d-4b9c-98db-d6a8aff047f0" ), "KOWALUK" );
        elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, 50, true );
    }

}
