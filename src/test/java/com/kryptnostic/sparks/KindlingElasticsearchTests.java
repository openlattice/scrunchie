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

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class KindlingElasticsearchTests extends BaseElasticsearchTest {
    
    @BeforeClass
    public static void createIndicesAndData() {
        elasticsearchApi.initializeEntitySetDataModelIndex();
        elasticsearchApi.initializeOrganizationIndex();
        elasticsearchApi.saveEntitySetToElasticsearch( chicagoEmployees, propertyTypesList, owner );
        elasticsearchApi.saveEntitySetToElasticsearch( entitySet2, propertyTypesList, owner );
        elasticsearchApi.createOrganization( organization, owner );
        createEntityData();
    }
    
    public static void createEntityData() {
        Map<UUID, String> propertyValues1 = Maps.newHashMap();
        propertyValues1.put( namePropertyId, "APOSTOLOS,  DIMITRIOS M" );
        propertyValues1.put( employeeTitlePropertyId, "ASST CHIEF OPERATING ENGINEER" );
        propertyValues1.put( employeeDeptPropertyId, "AVIATION" );
        propertyValues1.put( salaryPropertyId, "108534" );
        propertyValues1.put( employeeIdPropertyId, "12345" );
        Map<UUID, String> propertyValues2 = Maps.newHashMap();
        propertyValues2.put( namePropertyId, "ALVAREZ,  ROBERT" );
        propertyValues2.put( employeeTitlePropertyId, "POLICE OFFICER" );
        propertyValues2.put( employeeDeptPropertyId, "POLICE" );
        propertyValues2.put( salaryPropertyId, "81550" );
        propertyValues2.put( employeeIdPropertyId, "12346" );
        Map<UUID, String> propertyValues3 = Maps.newHashMap();
        propertyValues3.put( namePropertyId, "ALTMAN,  PATRICIA A" );
        propertyValues3.put( employeeTitlePropertyId, "POLICE OFFICER" );
        propertyValues3.put( employeeDeptPropertyId, "POLICE" );
        propertyValues3.put( salaryPropertyId, "93240" );
        propertyValues3.put( employeeIdPropertyId, "12347" );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, UUID.randomUUID().toString(), propertyValues1 );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, UUID.randomUUID().toString(), propertyValues2 );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, UUID.randomUUID().toString(), propertyValues3 );

        Map<UUID, String> entitySet2PropertyValues = Maps.newHashMap();
        entitySet2PropertyValues.put( employeeDeptPropertyId, "POLICE" );
        entitySet2PropertyValues.put( employeeIdPropertyId, "12347" );
        elasticsearchApi.createEntityData( entitySet2Id, UUID.randomUUID().toString(), entitySet2PropertyValues );
    }

    @Test
    public void testAddEntitySetPermissions() {
        Set<Permission> newPermissions = Sets.newHashSet();
        newPermissions.add( Permission.WRITE );
        newPermissions.add( Permission.READ );
        elasticsearchApi.updateEntitySetPermissions( chicagoEmployeesEntitySetId, loomUser, newPermissions );
    }

    @Test
    public void testEntitySetKeywordSearch() {
        Set<Principal> principals = Sets.newHashSet();
        principals.add( loomUser );

        String query = "Employees";
        elasticsearchApi.executeEntitySetDataModelKeywordSearch(
                Optional.of( query ),
                Optional.of( ENTITY_TYPE_ID ),
                Optional.absent(),
                principals );
    }

    @Test
    public void testUpdatePropertyTypes() {
        elasticsearchApi.updatePropertyTypesInEntitySet( chicagoEmployeesEntitySetId, allPropertyTypesList );
    }

    @Test
    public void testSearchEntityData() {
        Set<UUID> authorizedPropertyTypes = Sets.newHashSet();
        authorizedPropertyTypes.add( namePropertyId );
        authorizedPropertyTypes.add( employeeTitlePropertyId );
        authorizedPropertyTypes.add( employeeDeptPropertyId );
        authorizedPropertyTypes.add( salaryPropertyId );
        authorizedPropertyTypes.add( employeeIdPropertyId );
        elasticsearchApi.executeEntitySetDataSearch( chicagoEmployeesEntitySetId, "police", authorizedPropertyTypes );
    }

    @Test
    public void testSearchAcrossIndices() {
        Set<UUID> entitySetIds = Sets.newHashSet( chicagoEmployeesEntitySetId, entitySet2Id );
        Map<UUID, Set<String>> fieldSearches = Maps.newHashMap();
        fieldSearches.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, 50, true );
    }

    @Test
    public void testOrganizationKeywordSearch() {
        Set<Principal> principals = Sets.newHashSet();
        principals.add( owner );
        elasticsearchApi.executeOrganizationSearch( "loom", principals );
    }

    @Test
    public void testUpdateOrganization() throws InterruptedException {
        String newDescription = "this is a new description";
        elasticsearchApi.updateOrganization( organizationId, Optional.absent(), Optional.of( newDescription ) );
    }
    
    @AfterClass
    public static void deleteIndices() {
        elasticsearchApi.deleteEntitySet( chicagoEmployeesEntitySetId );
        elasticsearchApi.deleteEntitySet( entitySet2Id );
        elasticsearchApi.deleteOrganization( organizationId );
    }

}
