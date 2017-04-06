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
        elasticsearchApi.saveEntitySetToElasticsearch( chicagoEmployees, propertyTypesList, owner );
        elasticsearchApi.saveEntitySetToElasticsearch( entitySet2, propertyTypesList, owner );
        elasticsearchApi.createSecurableObjectIndex( chicagoEmployeesEntitySetId, SYNC_ID, propertyTypesList );
        elasticsearchApi.createSecurableObjectIndex( entitySet2Id, SYNC_ID2, propertyTypesList );
        elasticsearchApi.createOrganization( organization, owner );
        createEntityData();
    }

    public static void createEntityData() {
        Map<UUID, Object> propertyValues1 = Maps.newHashMap();
        propertyValues1.put( namePropertyId, Sets.newHashSet( "APOSTOLOS,  DIMITRIOS M" ) );
        propertyValues1.put( employeeTitlePropertyId, Sets.newHashSet( "ASST CHIEF OPERATING ENGINEER" ) );
        propertyValues1.put( employeeDeptPropertyId, Sets.newHashSet( "AVIATION" ) );
        propertyValues1.put( salaryPropertyId, Sets.newHashSet( "108534" ) );
        propertyValues1.put( employeeIdPropertyId, Sets.newHashSet( "12345" ) );
        Map<UUID, Object> propertyValues2 = Maps.newHashMap();
        propertyValues2.put( namePropertyId, Sets.newHashSet( "ALVAREZ,  ROBERT" ) );
        propertyValues2.put( employeeTitlePropertyId, Sets.newHashSet( "POLICE OFFICER" ) );
        propertyValues2.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        propertyValues2.put( salaryPropertyId, Sets.newHashSet( "81550" ) );
        propertyValues2.put( employeeIdPropertyId, Sets.newHashSet( "12346" ) );
        Map<UUID, Object> propertyValues3 = Maps.newHashMap();
        propertyValues3.put( namePropertyId, Sets.newHashSet( "ALTMAN,  PATRICIA A" ) );
        propertyValues3.put( employeeTitlePropertyId, Sets.newHashSet( "POLICE OFFICER" ) );
        propertyValues3.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        propertyValues3.put( salaryPropertyId, Sets.newHashSet( "93240" ) );
        propertyValues3.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, SYNC_ID, UUID.randomUUID().toString(), propertyValues1 );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, SYNC_ID, UUID.randomUUID().toString(), propertyValues2 );
        elasticsearchApi.createEntityData( chicagoEmployeesEntitySetId, SYNC_ID, UUID.randomUUID().toString(), propertyValues3 );

        Map<UUID, Object> entitySet2PropertyValues = Maps.newHashMap();
        entitySet2PropertyValues.put( employeeDeptPropertyId, Sets.newHashSet( "POLICE" ) );
        entitySet2PropertyValues.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi.createEntityData( entitySet2Id, SYNC_ID, UUID.randomUUID().toString(), entitySet2PropertyValues );
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
        elasticsearchApi.executeEntitySetMetadataSearch(
                Optional.of( query ),
                Optional.of( ENTITY_TYPE_ID ),
                Optional.absent(),
                principals,
                0,
                50 );
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
        elasticsearchApi.executeEntitySetDataSearch( chicagoEmployeesEntitySetId,
                SYNC_ID,
                "police",
                0,
                50,
                authorizedPropertyTypes );
    }

    @Test
    public void testSearchAcrossIndices() {
        Map<UUID, UUID> entitySetsAndSyncIds = Maps.newHashMap();
        entitySetsAndSyncIds.put( chicagoEmployeesEntitySetId, SYNC_ID );
        entitySetsAndSyncIds.put( entitySet2Id, SYNC_ID2 );
        Map<UUID, Set<String>> fieldSearches = Maps.newHashMap();
        fieldSearches.put( employeeIdPropertyId, Sets.newHashSet( "12347" ) );
        elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetsAndSyncIds, fieldSearches, 50, true );
    }

    @Test
    public void testOrganizationKeywordSearch() {
        Set<Principal> principals = Sets.newHashSet();
        principals.add( owner );
        elasticsearchApi.executeOrganizationSearch( "loom", principals, 0, 50 );
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
