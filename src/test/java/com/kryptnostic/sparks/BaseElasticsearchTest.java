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
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kryptnostic.conductor.rpc.ConductorConfiguration;
import com.kryptnostic.conductor.rpc.SearchConfiguration;
import com.kryptnostic.kindling.search.ConductorElasticsearchImpl;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService.StaticLoader;

public class BaseElasticsearchTest {
	
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
	    try {
            elasticsearchApi = new ConductorElasticsearchImpl( config );
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }
	}
}
