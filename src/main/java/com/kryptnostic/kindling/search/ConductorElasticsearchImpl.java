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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchDetails;
import com.dataloom.search.requests.SearchResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.SearchConfiguration;

public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {

    private static final Logger                 logger    = LoggerFactory.getLogger( ConductorElasticsearchImpl.class );
    private Client                              client;
    private ElasticsearchTransportClientFactory factory;
    private boolean                             connected = true;
    private String                              server;
    private String                              cluster;
    private int                                 port;

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
        initializeEntityTypeIndex();
        initializePropertyTypeIndex();
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

    private boolean initializeEntitySetDataModelIndex() {
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

    private boolean initializeOrganizationIndex() {
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

    private boolean initializeEntityTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( ENTITY_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( ENTITY_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( ENTITY_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ENTITY_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializePropertyTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( PROPERTY_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( PROPERTY_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( PROPERTY_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( PROPERTY_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private Map<String, String> getFieldMapping( PropertyType propertyType ) {
        Map<String, String> fieldMapping = Maps.newHashMap();
        switch ( propertyType.getDatatype() ) {
            case Binary: {
                fieldMapping.put( TYPE, BINARY );
                break;
            }
            case Boolean: {
                fieldMapping.put( TYPE, BOOLEAN );
                break;
            }
            case Byte: {
                fieldMapping.put( TYPE, BYTE );
                break;
            }
            case SByte: {
                fieldMapping.put( TYPE, BYTE );
                break;
            }
            case Decimal: {
                fieldMapping.put( TYPE, FLOAT );
                break;
            }
            case Single: {
                fieldMapping.put( TYPE, DOUBLE );
                break;
            }
            case Double: {
                fieldMapping.put( TYPE, DOUBLE );
                break;
            }
            case Guid: {
                fieldMapping.put( TYPE, KEYWORD );
                break;
            }
            case Int16: {
                fieldMapping.put( TYPE, SHORT );
                break;
            }
            case Int32: {
                fieldMapping.put( TYPE, INTEGER );
                break;
            }
            case Int64: {
                fieldMapping.put( TYPE, LONG );
                break;
            }
            case String: {
                String analyzer = ( propertyType.getAnalyzer().equals( Analyzer.METAPHONE ) ) ? METAPHONE_ANALYZER
                        : STANDARD;
                fieldMapping.put( TYPE, TEXT );
                fieldMapping.put( ANALYZER, analyzer );
                break;
            }
            case Date: {
                fieldMapping.put( TYPE, DATE );
                break;
            }
            case GeographyPoint: {
                fieldMapping.put( TYPE, GEO_POINT );
                break;
            }
            default: {
                fieldMapping.put( INDEX, "false" );
                fieldMapping.put( TYPE, KEYWORD );
            }
        }
        return fieldMapping;
    }

    private String getIndexName( UUID securableObjectId, UUID syncId ) {
        return SECURABLE_OBJECT_INDEX_PREFIX + securableObjectId.toString() + "_" + syncId.toString();
    }

    private String getTypeName( UUID securableObjectId ) {
        return SECURABLE_OBJECT_TYPE_PREFIX + securableObjectId.toString();
    }

    @Override
    public boolean createSecurableObjectIndex( UUID securableObjectId, UUID syncId, List<PropertyType> propertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        String indexName = getIndexName( securableObjectId, syncId );
        String typeName = getTypeName( securableObjectId );

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

        // securable_object_row type mapping
        Map<String, Object> securableObjectData = Maps.newHashMap();
        Map<String, Object> securableObjectMapping = Maps.newHashMap();
        Map<String, Object> properties = Maps.newHashMap();

        for ( PropertyType propertyType : propertyTypes ) {
            properties.put( propertyType.getId().toString(), getFieldMapping( propertyType ) );
        }

        securableObjectData.put( ES_PROPERTIES, properties );
        securableObjectMapping.put( typeName, securableObjectData );

        try {
            client.admin().indices().prepareCreate( indexName )
                    .setSettings( getMetaphoneSettings() )
                    .addMapping( typeName, securableObjectMapping )
                    .execute().actionGet();
        } catch ( IOException e ) {
            logger.debug( "unable to create securable object index" );
        }
        return true;
    }

    @Override
    public boolean saveEntitySetToElasticsearch(
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
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public SearchResult executeEntitySetMetadataSearch(
            Optional<String> optionalSearchTerm,
            Optional<UUID> optionalEntityType,
            Optional<Set<UUID>> optionalPropertyTypes,
            Set<Principal> principals,
            int start,
            int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
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
                .setFrom( start )
                .setSize( maxHits )
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
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
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
            client.prepareIndex( ENTITY_SET_DATA_MODEL, ACLS, id ).setParent( entitySetId.toString() ).setSource( s )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error updating entity set permissions in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
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
    public boolean deleteEntitySet( UUID entitySetId ) {
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

        client.admin().indices()
                .delete( new DeleteIndexRequest( SECURABLE_OBJECT_INDEX_PREFIX + entitySetId.toString() + "_*" ) );

        return true;
    }

    @Override
    public boolean deleteEntitySetForSyncId( UUID entitySetId, UUID syncId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.admin().indices()
                .delete( new DeleteIndexRequest( getIndexName( entitySetId, syncId ) ) );

        return true;
    }

    @Override
    public List<Entity> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
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
                    QueryBuilders.matchQuery( entry.getKey().toString(), searchTerm ).fuzziness( Fuzziness.AUTO )
                            .lenient( true ) ) );
            fieldQuery.minimumNumberShouldMatch( 1 );
            query.should( fieldQuery );
        } );
        query.minimumNumberShouldMatch( 1 );

        List<String> indexNames = entitySetAndSyncIds.entrySet().stream()
                .map( entry -> getIndexName( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList() );
        SearchResponse response = client.prepareSearch( indexNames.toArray( new String[ indexNames.size() ] ) )
                .setQuery( query )
                .setFrom( 0 )
                .setSize( size )
                .setExplain( explain )
                .execute()
                .actionGet();
        List<Entity> results = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            String[] entitySetIdAndSyncId = hit.getIndex().substring( SECURABLE_OBJECT_INDEX_PREFIX.length() ).split( "_" );
            UUID entitySetId = UUID.fromString( entitySetIdAndSyncId[0] );
            UUID syncId = UUID.fromString( entitySetIdAndSyncId[1] );
            EntityKey key = new EntityKey( entitySetId, hit.getId(), syncId );
            
            results.add( new Entity( key, hit.getSource() ) );
        }
        return results;
    }

    @Override
    public boolean updateOrganizationPermissions(
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
        } catch ( JsonProcessingException e ) {
            logger.debug( "error updating organization permissions in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean createEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            Map<UUID, Object> propertyValues ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        StringBuilder builder = new StringBuilder();
        Map<String, Object> paramValues = Maps.newHashMap();
        for ( Entry<UUID, Object> entry : propertyValues.entrySet() ) {
            List<Object> values = Lists.newArrayList( (Set<Object>) entry.getValue() );
            String id = entry.getKey().toString();
            paramValues.put( id, values );
            for ( int i = 0; i < values.size(); i++ ) {
                String paramName = id + "_" + String.valueOf( i );
                builder.append( "if (ctx._source['" )
                        .append( id )
                        .append( "'] == null) ctx._source['" )
                        .append( id )
                        .append( "'] = [params['" )
                        .append( paramName )
                        .append( "']]; else if (!ctx._source['" )
                        .append( id )
                        .append( "'].contains(params['" )
                        .append( paramName )
                        .append( "'])) ctx._source['" )
                        .append( id )
                        .append( "'].add(params['" + paramName + "']);" );
                paramValues.put( paramName, values.get( i ) );
                paramValues.put( paramName, values.get( i ) );
            }
        }

        Script script = new Script( ScriptType.INLINE, "painless", builder.toString(), paramValues );
        try {
            UpdateRequest request = new UpdateRequest(
                    getIndexName( entitySetId, syncId ),
                    getTypeName( entitySetId ),
                    entityId ).script( script )
                            .upsert( ObjectMappers.getJsonMapper().writeValueAsString( propertyValues ) );
            client.update( request ).actionGet();
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data in elasticsearch" );
            return false;
        }

        return true;
    }

    @Override
    public boolean deleteEntityData( UUID entitySetId, UUID syncId, String entityId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( getIndexName( entitySetId, syncId ), getTypeName( entitySetId ), entityId ).execute()
                .actionGet();
        return true;
    }

    @Override
    public boolean updateEntitySetMetadata( EntitySet entitySet ) {
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
            UpdateRequest updateRequest = new UpdateRequest(
                    ENTITY_SET_DATA_MODEL,
                    ENTITY_SET_TYPE,
                    entitySet.getId().toString() ).doc( s );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating entity set metadata in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean createOrganization( Organization organization, Principal principal ) {
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
    public boolean deleteOrganization( UUID organizationId ) {
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
    public SearchResult executeEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            String searchTerm,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        Map<String, Float> fieldsMap = Maps.newHashMap();
        String[] authorizedPropertyTypeFields = authorizedPropertyTypes
                .stream()
                .map( uuid -> {
                    fieldsMap.put( uuid.toString(), Float.valueOf( "1" ) );
                    return uuid.toString();
                } )
                .collect( Collectors.toList() )
                .toArray( new String[ authorizedPropertyTypes.size() ] );

        QueryStringQueryBuilder query = QueryBuilders.queryStringQuery( searchTerm ).fields( fieldsMap )
                .lenient( true );
        SearchResponse response = client.prepareSearch( getIndexName( entitySetId, syncId ) )
                .setQuery( query )
                .setFetchSource( authorizedPropertyTypeFields, null )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();
        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            Map<String, Object> result = hit.getSource();
            result.put( ID, hit.getId() );
            hits.add( result );
        }
        SearchResult result = new SearchResult( response.getHits().totalHits(), hits );
        return result;
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<Principal> principals,
            int start,
            int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
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
                .setFrom( start )
                .setSize( maxHits )
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
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
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

    @Override
    public SearchResult executeAdvancedEntitySetDataSearch(
            UUID entitySetId,
            UUID syncId,
            List<SearchDetails> searches,
            int start,
            int maxHits,
            Set<UUID> authorizedPropertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        Map<String, Float> fieldsMap = Maps.newHashMap();
        String[] authorizedPropertyTypeFields = authorizedPropertyTypes
                .stream()
                .map( uuid -> {
                    fieldsMap.put( uuid.toString(), Float.valueOf( "1" ) );
                    return uuid.toString();
                } )
                .collect( Collectors.toList() )
                .toArray( new String[ authorizedPropertyTypes.size() ] );

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumNumberShouldMatch( 1 );
        searches.forEach( search -> {
            QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery( search.getSearchTerm() )
                    .field( search.getPropertyType().toString(), Float.valueOf( "1" ) ).lenient( true );
            if ( search.getExactMatch() ) {
                query.must( queryString );
                query.minimumNumberShouldMatch( 0 );
            }
            else query.should( queryString );
        } );

        SearchResponse response = client.prepareSearch( getIndexName( entitySetId, syncId ) )
                .setQuery( query )
                .setFetchSource( authorizedPropertyTypeFields, null )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();
        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        SearchResult result = new SearchResult( response.getHits().totalHits(), hits );
        return result;
    }

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entityType );
            client.prepareIndex( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityType.getId().toString() ).setSource( s )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyType );
            client.prepareIndex( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyType.getId().toString() ).setSource( s )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving entity set to elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean deleteEntityType( UUID entityTypeId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypeId.toString() ).execute().actionGet();
        return true;
    }

    @Override
    public boolean deletePropertyType( UUID propertyTypeId ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return false;
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypeId.toString() ).execute().actionGet();
        return true;
    }

    @Override
    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.should( QueryBuilders.matchQuery( TYPE + "." + NAME, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( TYPE + "." + NAMESPACE, searchTerm ) )
                .should( QueryBuilders.matchQuery( TITLE, searchTerm )
                        .fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm )
                        .fuzziness( Fuzziness.AUTO ) )
                .minimumNumberShouldMatch( 1 );

        SearchResponse response = client.prepareSearch( ENTITY_TYPE_INDEX )
                .setTypes( ENTITY_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.must( QueryBuilders.regexpQuery( TYPE + "." + NAMESPACE, ".*" + namespace + ".*" ) )
                .must( QueryBuilders.regexpQuery( TYPE + "." + NAME, ".*" + name + ".*" ) );

        SearchResponse response = client.prepareSearch( ENTITY_TYPE_INDEX )
                .setTypes( ENTITY_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.should( QueryBuilders.matchQuery( TYPE + "." + NAME, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( TYPE + "." + NAMESPACE, searchTerm ) )
                .should( QueryBuilders.matchQuery( TITLE, searchTerm )
                        .fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm )
                        .fuzziness( Fuzziness.AUTO ) )
                .minimumNumberShouldMatch( 1 );

        SearchResponse response = client.prepareSearch( PROPERTY_TYPE_INDEX )
                .setTypes( PROPERTY_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() )
                return new SearchResult( 0, Lists.newArrayList() );
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.must( QueryBuilders.regexpQuery( TYPE + "." + NAMESPACE, ".*" + namespace + ".*" ) )
                .must( QueryBuilders.regexpQuery( TYPE + "." + NAME, ".*" + name + ".*" ) );

        SearchResponse response = client.prepareSearch( PROPERTY_TYPE_INDEX )
                .setTypes( PROPERTY_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSource() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

}
