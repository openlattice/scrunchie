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

package com.kryptnostic.kindling.search;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.codec.language.Metaphone;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.organization.Organization;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.SearchConfiguration;


public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {

    private static final Logger logger = LoggerFactory.getLogger( ConductorElasticsearchImpl.class );
    private Client client;
    private ElasticsearchTransportClientFactory factory;
    private boolean connected = true;
    private String server;
    private String cluster;
    private int    port;

    public ConductorElasticsearchImpl( SearchConfiguration config ) throws UnknownHostException {
        this( config, Optional.absent() );
    }

    public ConductorElasticsearchImpl(
            SearchConfiguration config,
            Client someClient ) throws UnknownHostException {
        this( config, Optional.of( someClient ) );
    }

    public ConductorElasticsearchImpl(
            SearchConfiguration config,
            Optional<Client> someClient ) throws UnknownHostException {
        init( config );
        client = someClient.or( factory.getClient() );
        initializeIndices();
    }

    private void init( SearchConfiguration config ) {
        server = config.getElasticsearchUrl();
        cluster = config.getElasticsearchCluster();
        port = config.getElasticsearchPort();
        factory = new ElasticsearchTransportClientFactory( server, port, cluster );
    }

    public void initializeIndices() {
        initializeEntitySetDataModelIndex();
        initializeOrganizationIndex();
    }
    
    private XContentBuilder getMetaphoneSettings() throws IOException {
    	XContentBuilder settings = XContentFactory.jsonBuilder()
    	        .startObject()
        	        .startObject( ANALYSIS )
                        .startObject( FILTER )
                            .startObject( METAPHONE_FILTER )
                                .field( TYPE, PHONETIC )
                                .field( ENCODER, METAPHONE )
                                .field( REPLACE, false )
                            .endObject()
                        .endObject()
            	        .startObject( ANALYZER )
                	        .startObject( METAPHONE_ANALYZER )
                	            .field( TOKENIZER, STANDARD )
                	            .field( FILTER, Lists.newArrayList( STANDARD, LOWERCASE, METAPHONE_FILTER ) )
                	        .endObject()
                	    .endObject()
        	        .endObject()
        	        .field( NUM_SHARDS, 3 )
        	        .field( NUM_REPLICAS, 3 )
    	        .endObject();
    	return settings;
    }

    @Override
    public Boolean initializeEntitySetDataModelIndex() {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( ENTITY_SET_DATA_MODEL ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // constant Map<String, String> type fields
        Map<String, String> objectField = Maps.newHashMap();
        Map<String, String> nestedField = Maps.newHashMap();
        Map<String, String> keywordField = Maps.newHashMap();
        Map<String, Object> aclParent = Maps.newHashMap();
        objectField.put( TYPE, OBJECT );
        nestedField.put( TYPE, NESTED );
        keywordField.put( TYPE, KEYWORD );
        aclParent.put( TYPE, ENTITY_SET_TYPE );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> entitySetData = Maps.newHashMap();
        Map<String, Object> mapping = Maps.newHashMap();
        properties.put( PROPERTY_TYPES, nestedField );
        properties.put( ENTITY_SET, objectField );
        entitySetData.put( ES_PROPERTIES, properties );
        mapping.put( ENTITY_SET_TYPE, entitySetData );

        // acl type mapping
        Map<String, Object> aclProperties = Maps.newHashMap();
        Map<String, Object> aclData = Maps.newHashMap();
        Map<String, Object> aclMapping = Maps.newHashMap();
        aclProperties.put( ACLS, keywordField );
        aclProperties.put( TYPE, keywordField );
        aclProperties.put( NAME, keywordField );
        aclProperties.put( ENTITY_SET_ID, keywordField );
        aclData.put( ES_PROPERTIES, aclProperties );
        aclData.put( PARENT, aclParent );
        aclMapping.put( ACLS, aclData );

        client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ENTITY_SET_TYPE, mapping )
                .addMapping( ACLS, aclMapping )
                .execute().actionGet();
        return true;
    }

    @Override
    public Boolean initializeOrganizationIndex() {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( ORGANIZATIONS ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // constant Map<String, String> type fields
        Map<String, String> objectField = Maps.newHashMap();
        Map<String, String> keywordField = Maps.newHashMap();
        Map<String, Object> aclParent = Maps.newHashMap();
        objectField.put( TYPE, OBJECT );
        keywordField.put( TYPE, KEYWORD );
        aclParent.put( TYPE, ORGANIZATION_TYPE );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> organizationData = Maps.newHashMap();
        Map<String, Object> organizationMapping = Maps.newHashMap();
        properties.put( ORGANIZATION, objectField );
        organizationData.put( ES_PROPERTIES, properties );
        organizationMapping.put( ORGANIZATION_TYPE, organizationData );

        // acl type mapping
        Map<String, Object> aclProperties = Maps.newHashMap();
        Map<String, Object> aclData = Maps.newHashMap();
        Map<String, Object> aclMapping = Maps.newHashMap();
        aclProperties.put( ACLS, keywordField );
        aclProperties.put( TYPE, keywordField );
        aclProperties.put( NAME, keywordField );
        aclProperties.put( ORGANIZATION_ID, keywordField );
        aclData.put( ES_PROPERTIES, aclProperties );
        aclData.put( PARENT, aclParent );
        aclMapping.put( ACLS, aclData );

        client.admin().indices().prepareCreate( ORGANIZATIONS )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ORGANIZATION_TYPE, organizationMapping )
                .addMapping( ACLS, aclMapping )
                .execute().actionGet();
        return true;
    }

    public Boolean createSecurableObjectIndex( UUID securableObjectId, List<PropertyType> propertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        String indexName = SECURABLE_OBJECT_INDEX_PREFIX + securableObjectId.toString();

        boolean exists = client.admin().indices()
                .prepareExists( indexName ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        // constant Map<String, String> type fields
        Map<String, String> objectField = Maps.newHashMap();
        Map<String, String> nestedField = Maps.newHashMap();
        Map<String, String> keywordField = Maps.newHashMap();
        objectField.put( TYPE, OBJECT );
        nestedField.put( TYPE, NESTED );
        keywordField.put( TYPE, KEYWORD );

      //   securable_object_row type mapping
        Map<String, Object> securableObjectData = Maps.newHashMap();
        Map<String, Object> securableObjectMapping = Maps.newHashMap();
        Map<String, Object> properties = Maps.newHashMap();
        
        Map<String, String> metaphoneTypeAndAnalyzer = Maps.newHashMap();        
        metaphoneTypeAndAnalyzer.put( TYPE, STRING );
        metaphoneTypeAndAnalyzer.put( ANALYZER, METAPHONE_ANALYZER );
        
        // TODO: once property type phonetic field exists, add analyzer if it's set to true
        
//        for ( PropertyType propertyType: propertyTypes ) {
//            properties.put( propertyType.getId().toString(), metaphoneTypeAndAnalyzer );
//        }
//        
        securableObjectData.put( ES_PROPERTIES, properties );
        securableObjectMapping.put( SECURABLE_OBJECT_ROW_TYPE, securableObjectData );

        try {
            client.admin().indices().prepareCreate( indexName )
                    .setSettings( getMetaphoneSettings() )
                    .addMapping( SECURABLE_OBJECT_ROW_TYPE, securableObjectMapping )
                    .execute().actionGet();
        } catch ( IOException e ) {
            logger.debug( "unable to create securable object index" );
        }
        return true;
    }

    @Override
    public Boolean saveEntitySetToElasticsearch(
            EntitySet entitySet,
            List<PropertyType> propertyTypes,
            Principal principal ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> entitySetDataModel = Maps.newHashMap();
        entitySetDataModel.put( ENTITY_SET, entitySet );
        entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetDataModel );
            client.prepareIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() ).setSource( s )
                    .execute().actionGet();
            updateEntitySetPermissions(
                    entitySet.getId(),
                    principal,
                    Sets.newHashSet( Permission.OWNER,
                            Permission.READ,
                            Permission.WRITE,
                            Permission.DISCOVER,
                            Permission.LINK ) );
            createSecurableObjectIndex( entitySet.getId(), propertyTypes );
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public List<Map<String, Object>> executeEntitySetDataModelKeywordSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<Principal> principals ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return Lists.newArrayList();
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        BoolQueryBuilder permissionsQuery = new BoolQueryBuilder();
        for ( Principal principal : principals ) {
            BoolQueryBuilder childQuery = new BoolQueryBuilder();
            childQuery.must( QueryBuilders.matchQuery( NAME, principal.getId() ) );
            childQuery.must( QueryBuilders.matchQuery( TYPE, principal.getType().toString() ) );
            childQuery.must( QueryBuilders.termQuery( ACLS, Permission.READ.toString() ) );
            String hitName = "acl_" + principal.getType().toString() + "_" + principal.getId();
            permissionsQuery.should( QueryBuilders.hasChildQuery( ACLS, childQuery, ScoreMode.Avg )
                    .innerHit( new InnerHitBuilder()
                            .setFetchSourceContext( new FetchSourceContext( true, new String[] { ACLS }, null ) )
                            .setName( hitName ) ) );
        }
        permissionsQuery.minimumNumberShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( permissionsQuery );

        if ( optionalSearchTerm.isPresent() ) {
            String searchTerm = optionalSearchTerm.get();
            query.should( QueryBuilders.matchQuery( ENTITY_SET + "." + NAME, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                    .should( QueryBuilders.matchQuery( ENTITY_SET + "." + TITLE, searchTerm )
                            .fuzziness( Fuzziness.AUTO ) )
                    .should( QueryBuilders.matchQuery( ENTITY_SET + "." + DESCRIPTION, searchTerm )
                            .fuzziness( Fuzziness.AUTO ) )
                    .minimumNumberShouldMatch( 1 );
        }

        if ( optionalEntityType.isPresent() ) {
            UUID eid = optionalEntityType.get();
            query.must( QueryBuilders.matchQuery( ENTITY_SET + "." + ENTITY_TYPE_ID, eid.toString() ) );
        } else if ( optionalPropertyTypes.isPresent() ) {
            Set<UUID> propertyTypes = optionalPropertyTypes.get();
            for ( UUID pid : propertyTypes ) {
                query.must( QueryBuilders.nestedQuery( PROPERTY_TYPES,
                        QueryBuilders.matchQuery( PROPERTY_TYPES + "." + ID, pid.toString() ),
                        ScoreMode.Avg ) );
            }
        }
        SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
                .setTypes( ENTITY_SET_TYPE )
                .setQuery( query )
                .setFetchSource( new String[] { ENTITY_SET, PROPERTY_TYPES }, null )
                .setFrom( 0 ).setSize( 50 ).setExplain( true )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            Map<String, Object> match = hit.getSource();
            Set<String> permissions = Sets.newHashSet();
            for ( SearchHits innerHits : hit.getInnerHits().values() ) {
                for ( SearchHit innerHit : innerHits.getHits() ) {
                    permissions.addAll( (List<String>) innerHit.getSource().get( ACLS ) );
                }
            }
            match.put( ACLS, permissions );
            hits.add( match );
        }
        return hits;
    }

    @Override
    public Boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> acl = Maps.newHashMap();
        acl.put( ACLS, permissions );
        acl.put( TYPE, principal.getType().toString() );
        acl.put( NAME, principal.getId() );
        acl.put( ENTITY_SET_ID, entitySetId.toString() );
		try {
			String s = ObjectMappers.getJsonMapper().writeValueAsString( acl );
			String id = entitySetId.toString() + "_" + principal.getType().toString() + "_" + principal.getId();
			client.prepareIndex( ENTITY_SET_DATA_MODEL, ACLS, id ).setParent( entitySetId.toString() ).setSource( s ).execute().actionGet();
			return true;
		} catch (JsonProcessingException e) {
			logger.debug( "error updating entity set permissions in elasticsearch" );
		}
		return false;
	}

    @Override
    public Boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        Map<String, Object> propertyTypes = Maps.newHashMap();
        propertyTypes.put( PROPERTY_TYPES, newPropertyTypes );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyTypes );
            UpdateRequest updateRequest = new UpdateRequest(
                    ENTITY_SET_DATA_MODEL,
                    ENTITY_SET_TYPE,
                    entitySetId.toString() ).doc( s );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating property types of entity set in elasticsearch" );
        }
        return false;
    }

    @Override
    public Boolean deleteEntitySet( UUID entitySetId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).execute().actionGet();

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE ).filter(
                QueryBuilders.boolQuery()
                        .must( QueryBuilders.matchQuery( TYPE_FIELD, ACLS ) )
                        .must( QueryBuilders.matchQuery( ENTITY_SET_ID, entitySetId.toString() ) ) )
                .source( ENTITY_SET_DATA_MODEL )
                .execute()
                .actionGet();
        
        client.admin().indices().delete( new DeleteIndexRequest( SECURABLE_OBJECT_INDEX_PREFIX + entitySetId.toString() ) );

        return true;
    }

    @Override
    public List<Entity> executeEntitySetDataSearchAcrossIndices(
            Set<UUID> entitySetIds,
            Map<UUID, Set<String>> fieldSearches,
            int size,
            boolean explain ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return null;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        BoolQueryBuilder query = new BoolQueryBuilder();
        fieldSearches.entrySet().stream().forEach( entry -> {
        	BoolQueryBuilder fieldQuery = new BoolQueryBuilder();
        	entry.getValue().stream().forEach( searchTerm -> fieldQuery.should(
        			QueryBuilders.matchQuery( entry.getKey().toString(), searchTerm ).fuzziness( Fuzziness.AUTO ) ) );
        	fieldQuery.minimumNumberShouldMatch( 1 );
        	query.should( fieldQuery );
        });
        query.minimumNumberShouldMatch( 1 );

        List<String> indexNames = entitySetIds.stream().map( id -> SECURABLE_OBJECT_INDEX_PREFIX + id.toString() )
                .collect( Collectors.toList() );
        SearchResponse response = client.prepareSearch( indexNames.toArray( new String[ indexNames.size() ] ) )
                .setTypes( SECURABLE_OBJECT_ROW_TYPE )
                .setQuery( query )
                .setFrom( 0 )
                .setSize( size )
                .setExplain( explain )
                .execute()
                .actionGet();
        List<Entity> results = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
        	UUID entitySetId = UUID.fromString( hit.getIndex().substring( SECURABLE_OBJECT_INDEX_PREFIX.length() ) );
        	EntityKey key = new EntityKey( entitySetId, hit.getId() );
            results.add( new Entity( key, hit.getSource() ) );
        }
        return results;
    }

    @Override
    public Boolean updateOrganizationPermissions(
            UUID organizationId,
            Principal principal,
            Set<Permission> permissions ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> acl = Maps.newHashMap();
        acl.put( ACLS, permissions );
        acl.put( TYPE, principal.getType().toString() );
        acl.put( NAME, principal.getId() );
        acl.put( ORGANIZATION_ID, organizationId.toString() );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( acl );
            String id = organizationId.toString() + "_" + principal.getType().toString() + "_" + principal.getId();
            client.prepareIndex( ORGANIZATIONS, ACLS, id ).setParent( organizationId.toString() ).setSource( s )
                    .execute().actionGet();
            return true;
        } catch (JsonProcessingException e) {
            logger.debug( "error updating organization permissions in elasticsearch" );
        }
        return false;
    }

    @Override
    public Boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, String> propertyValues ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        String indexName = SECURABLE_OBJECT_INDEX_PREFIX + entitySetId.toString();

        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyValues );
            client
                    .prepareIndex( indexName, SECURABLE_OBJECT_ROW_TYPE, entityId )
                    .setSource( s )
                    .execute()
                    .actionGet();
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data in elasticsearch" );
            return false;
        }

        return true;
    }

    @Override
    public Boolean updateEntitySetMetadata( EntitySet entitySet ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        Map<String, Object> entitySetObj = Maps.newHashMap();
        entitySetObj.put( ENTITY_SET, entitySet );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetObj );
            UpdateRequest updateRequest = new UpdateRequest( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() ).doc( s );
            client.update( updateRequest ).actionGet();
            return true;
        } catch (IOException e) {
            logger.debug( "error updating entity set metadata in elasticsearch" );
        }
        return false;
    }

    @Override
    public Boolean createOrganization( Organization organization, Principal principal ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> organizationObject = Maps.newHashMap();
        organizationObject.put( TITLE, organization.getTitle() );
        organizationObject.put( DESCRIPTION, organization.getDescription() );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( organizationObject );
            client.prepareIndex( ORGANIZATIONS, ORGANIZATION_TYPE, organization.getId().toString() ).setSource( s )
                    .execute().actionGet();
            updateOrganizationPermissions(
                    organization.getId(),
                    principal,
                    Sets.newHashSet( Permission.OWNER,
                            Permission.READ,
                            Permission.WRITE,
                            Permission.DISCOVER,
                            Permission.LINK ) );
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating organization in elasticsearch" );
        }
        return false;
    }

    @Override
    public Boolean deleteOrganization( UUID organizationId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( ORGANIZATIONS, ORGANIZATION_TYPE, organizationId.toString() ).execute().actionGet();

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE ).filter(
                QueryBuilders.boolQuery()
                        .must( QueryBuilders.matchQuery( TYPE_FIELD, ACLS ) )
                        .must( QueryBuilders.matchQuery( ORGANIZATION_ID, organizationId.toString() ) ) )
                .source( ORGANIZATIONS )
                .execute()
                .actionGet();

        return true;
    }

    @Override
    public List<Map<String, Object>> executeEntitySetDataSearch(
            UUID entitySetId,
            String searchTerm,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return Lists.newArrayList();
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        String indexName = SECURABLE_OBJECT_INDEX_PREFIX + entitySetId.toString();
        Map<String, Float> fieldsMap = Maps.newHashMap();
        String[] authorizedPropertyTypeFields = authorizedPropertyTypes
                .stream()
                .map( uuid -> {
                    fieldsMap.put( uuid.toString(), Float.valueOf( "1" ) );
                    return uuid.toString();
                })
                .collect( Collectors.toList() )
                .toArray( new String[ authorizedPropertyTypes.size() ] );

        QueryStringQueryBuilder query = QueryBuilders.queryStringQuery( searchTerm ).fields( fieldsMap );
        SearchResponse response = client.prepareSearch( indexName )
                .setTypes( SECURABLE_OBJECT_ROW_TYPE )
                .setQuery( query )
                .setFetchSource( authorizedPropertyTypeFields, null )
                .setFrom( 0 )
                .setSize( 50 )
                .execute()
                .actionGet();
        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        return hits;
    }

    @Override
    public List<Map<String, Object>> executeOrganizationSearch( String searchTerm, Set<Principal> principals ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return Lists.newArrayList();
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder permissionsQuery = new BoolQueryBuilder();
        for ( Principal principal : principals ) {
            BoolQueryBuilder childQuery = new BoolQueryBuilder();
            childQuery.must( QueryBuilders.matchQuery( NAME, principal.getId() ) );
            childQuery.must( QueryBuilders.matchQuery( TYPE, principal.getType().toString() ) );
            childQuery.must( QueryBuilders.termQuery( ACLS, Permission.READ.toString() ) );
            String hitName = "acl_" + principal.getType().toString() + "_" + principal.getId();
            permissionsQuery.should( QueryBuilders.hasChildQuery( ACLS, childQuery, ScoreMode.Avg )
                    .innerHit( new InnerHitBuilder()
                            .setFetchSourceContext( new FetchSourceContext( true, new String[] { ACLS }, null ) )
                            .setName( hitName ) ) );
        }
        permissionsQuery.minimumNumberShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( permissionsQuery )
                .should( QueryBuilders.matchQuery( TITLE, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .minimumNumberShouldMatch( 1 );

        SearchResponse response = client.prepareSearch( ORGANIZATIONS )
                .setTypes( ORGANIZATION_TYPE )
                .setQuery( query )
                .setFrom( 0 ).setSize( 50 ).setExplain( true )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            Map<String, Object> match = hit.getSource();
            match.put( ID, hit.getId() );
            Set<String> permissions = new HashSet<>();
            for ( SearchHits innerHits : hit.getInnerHits().values() ) {
                for ( SearchHit innerHit : innerHits.getHits() ) {
                    permissions.addAll( (List<String>) innerHit.getSource().get( ACLS ) );
                }
            }
            match.put( ACLS, permissions );
            hits.add( match );
        }
        return hits;
    }

    @Override
    public Boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        Map<String, Object> updatedFields = Maps.newHashMap();
        if ( optionalTitle.isPresent() ) {
            updatedFields.put( TITLE, optionalTitle.get() );
        }
        if ( optionalDescription.isPresent() ) {
            updatedFields.put( DESCRIPTION, optionalDescription.get() );
        }
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( updatedFields );
            UpdateRequest updateRequest = new UpdateRequest( ORGANIZATIONS, ORGANIZATION_TYPE, id.toString() ).doc( s );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating organization in elasticsearch" );
        }
        return false;
    }

    public boolean verifyElasticsearchConnection() throws UnknownHostException {
        if ( connected ) {
            if ( !factory.isConnected( client ) ) {
                connected = false;
            }
        } else {
            client = factory.getClient();
            if ( client != null ) {
                connected = true;
            }
        }
        return connected;
    }

    @Scheduled(
            fixedRate = 1800000 )
    public void verifyRunner() throws UnknownHostException {
        verifyElasticsearchConnection();
    }

}
