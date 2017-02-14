/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.sparks;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Lists;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.organization.Organization;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.ConductorConfiguration;
import com.kryptnostic.conductor.rpc.SearchConfiguration;
import com.kryptnostic.kindling.search.ConductorElasticsearchImpl;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService.StaticLoader;

public class BaseElasticsearchTest {
    
    protected static UUID namePropertyId = UUID.fromString( "12926a46-7b2d-4b9c-98db-d6a8aff047f0" );
    protected static UUID employeeIdPropertyId = UUID.fromString( "65d76d13-0d91-4d78-8dbd-cf6ce6e6162f" );
    protected static UUID salaryPropertyId = UUID.fromString( "60de791c-df3e-462b-8299-ea36dc3beb16" );
    protected static UUID employeeDeptPropertyId = UUID.fromString( "4328a8e7-16e1-42a3-ad5b-adf4b06921ec" );
    protected static UUID employeeTitlePropertyId = UUID.fromString( "4a6f084d-cd44-4d5b-9188-947d7151bf84" );
    protected static List<PropertyType> propertyTypesList = Lists.newArrayList();
    protected static List<PropertyType> allPropertyTypesList = Lists.newArrayList();
    
    protected static UUID chicagoEmployeesEntitySetId = UUID.fromString( "15d8f726-74eb-420f-b63e-9774ebc95c3f" );
    protected static UUID entitySet2Id = UUID.fromString( "4c767353-8fcc-4b37-9ff9-bb3ad0ab96e4" );
    
    protected static UUID organizationId = UUID.fromString( "93e64078-d1a4-4306-a66c-2448d2fd3504" );
    
    protected static PropertyType name;
    protected static PropertyType id;
    protected static PropertyType salary;
    protected static PropertyType dept;
    protected static PropertyType title;
    
    protected static EntitySet chicagoEmployees;
    protected static EntitySet entitySet2;
    
    protected static Principal owner;
    protected static Principal loomUser;
    
    protected static Organization organization;
    	
	protected static final UUID   ENTITY_SET_ID  = UUID.fromString( "0a648f39-5e41-46b5-a928-ec44cdeeae13" );
    protected static final UUID   ENTITY_TYPE_ID = UUID.fromString( "c271a300-ea05-420b-b33b-8ecb18de5ce7" );
    protected static final String TITLE          = "The Entity Set Title";
    protected static final String DESCRIPTION    = "This is a description for the entity set called employees.";
    
    protected static final String NAMESPACE                = "testcsv";
    protected static final String SALARY                   = "salary";
    protected static final String EMPLOYEE_NAME            = "employee_name";
    protected static final String EMPLOYEE_TITLE           = "employee_title";
    protected static final String EMPLOYEE_DEPT            = "employee_dept";
    protected static final String EMPLOYEE_ID              = "employee_id";
    protected static final String WEIGHT					 = "weight";
    protected static final String ENTITY_SET_NAME          = "Employees";
    protected static final FullQualifiedName    ENTITY_TYPE              = new FullQualifiedName(
            NAMESPACE,
            "employee" );
    
    protected static final int ELASTICSEARCH_PORT = 9300;
    protected static final String ELASTICSEARCH_CLUSTER = "loom_development";
    protected static final String ELASTICSEARCH_URL = "localhost";
	protected static ConductorElasticsearchImpl elasticsearchApi;
	protected static final Logger logger = LoggerFactory.getLogger( BaseElasticsearchTest.class );
	
	@BeforeClass
	public static void init() {
	    SearchConfiguration config = StaticLoader.loadConfiguration( ConductorConfiguration.class ).getSearchConfiguration();
	    initEdmObjects();
	    try {
            elasticsearchApi = new ConductorElasticsearchImpl( config );
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }
	}
	
	public static void initEdmObjects() {
	    name = new PropertyType(
	            namePropertyId,
                new FullQualifiedName( "elasticsearchtest", "name" ),
                "Name",
                Optional.of( "Employee Name" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        title = new PropertyType(
                employeeTitlePropertyId,
                new FullQualifiedName( "elasticsearchtest", "title" ),
                "Title",
                Optional.of( "Employee Title" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        dept = new PropertyType(
                employeeDeptPropertyId,
                new FullQualifiedName( "elasticsearchtest", "dept" ),
                "Dept",
                Optional.of( "Employee Department" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String );
        salary = new PropertyType(
                salaryPropertyId,
                new FullQualifiedName( "elasticsearchtest", "salary" ),
                "Salary",
                Optional.of( "Employee Salary" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int64 );
        id = new PropertyType(
                employeeIdPropertyId,
                new FullQualifiedName( "elasticsearchtest", "id" ),
                "Id",
                Optional.of( "Employee Id" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int64 );
        propertyTypesList.add( dept );
        propertyTypesList.add( id );
        allPropertyTypesList.add( name );
        allPropertyTypesList.add( title );
        allPropertyTypesList.add( dept );
        allPropertyTypesList.add( salary );
        allPropertyTypesList.add( id );
        
        chicagoEmployees = new EntitySet(
                Optional.of( chicagoEmployeesEntitySetId ),
                ENTITY_TYPE_ID,
                "chicago_employees",
                "Chicago Employees",
                Optional.of( "employees that are in chicago" ) );
        entitySet2 = new EntitySet(
                Optional.of( entitySet2Id ),
                ENTITY_TYPE_ID,
                "entity_set2",
                "EntitySet2",
                Optional.of( "this is the second entity set" ) );
        
        owner = new Principal( PrincipalType.USER, "support@kryptnostic.com" );
        loomUser = new Principal( PrincipalType.ROLE, "loomUser" );
        
        organization = new Organization(
                Optional.of( organizationId ),
                "Loom Employees",
                Optional.of( "people that work at loom" ),
                Optional.absent(),
                Sets.newHashSet(),
                Sets.newHashSet(),
                Sets.newHashSet() );
        
	}
}
