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

import com.dataloom.apps.App;
import com.dataloom.apps.AppType;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.data.EntityKey;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchDetails;
import com.dataloom.search.requests.SearchResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.SearchConfiguration;
import com.openlattice.authorization.AclKey;
import com.openlattice.rhizome.hazelcast.DelegatedStringSet;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.util.ModelSerializer;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.nd4j.linalg.factory.Nd4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {

    private static final Logger logger = LoggerFactory.getLogger( ConductorElasticsearchImpl.class );
    private final MultiLayerNetwork                   net;
    private final ThreadLocal                         modelThread;
    private       Client                              client;
    private       ElasticsearchTransportClientFactory factory;
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

        MultiLayerNetwork network;
        try {
            network = ModelSerializer
                    .restoreMultiLayerNetwork(
                            Thread.currentThread().getContextClassLoader().getResourceAsStream( "model.bin" ) );
        } catch ( IOException e ) {
            network = null;
            logger.error( "Unable to load neural net", e );
        }
        this.net = network;
        modelThread = ThreadLocal.withInitial( () -> net.clone() );
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
        initializeAssociationTypeIndex();
        initializePropertyTypeIndex();
        initializeAppIndex();
        initializeAppTypeIndex();
    }

    // @formatter:off
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
 // @formatter:on

    private boolean initializeEntitySetDataModelIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
        objectField.put( TYPE, OBJECT );
        nestedField.put( TYPE, NESTED );
        keywordField.put( TYPE, KEYWORD );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> entitySetData = Maps.newHashMap();
        Map<String, Object> mapping = Maps.newHashMap();
        properties.put( PROPERTY_TYPES, nestedField );
        properties.put( ENTITY_SET, objectField );
        entitySetData.put( ES_PROPERTIES, properties );
        mapping.put( ENTITY_SET_TYPE, entitySetData );

        client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ENTITY_SET_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeOrganizationIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
        objectField.put( TYPE, OBJECT );
        keywordField.put( TYPE, KEYWORD );

        // entity_set type mapping
        Map<String, Object> properties = Maps.newHashMap();
        Map<String, Object> organizationData = Maps.newHashMap();
        Map<String, Object> organizationMapping = Maps.newHashMap();
        properties.put( ORGANIZATION, objectField );
        organizationData.put( ES_PROPERTIES, properties );
        organizationMapping.put( ORGANIZATION_TYPE, organizationData );

        client.admin().indices().prepareCreate( ORGANIZATIONS )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ORGANIZATION_TYPE, organizationMapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeEntityTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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

    private boolean initializeAssociationTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( ASSOCIATION_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( ASSOCIATION_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( ASSOCIATION_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( ASSOCIATION_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializePropertyTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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

    private boolean initializeAppIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( APP_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( APP, Maps.newHashMap() );
        client.admin().indices().prepareCreate( APP_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( APP, mapping )
                .execute().actionGet();
        return true;
    }

    private boolean initializeAppTypeIndex() {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            e.printStackTrace();
        }

        boolean exists = client.admin().indices()
                .prepareExists( APP_TYPE_INDEX ).execute().actionGet().isExists();
        if ( exists ) {
            return true;
        }

        Map<String, Object> mapping = Maps.newHashMap();
        mapping.put( APP_TYPE, Maps.newHashMap() );
        client.admin().indices().prepareCreate( APP_TYPE_INDEX )
                .setSettings( Settings.builder()
                        .put( NUM_SHARDS, 3 )
                        .put( NUM_REPLICAS, 2 ) )
                .addMapping( APP_TYPE, mapping )
                .execute().actionGet();
        return true;
    }

    private Map<String, String> getFieldMapping( PropertyType propertyType ) {
        Map<String, String> fieldMapping = Maps.newHashMap();
        switch ( propertyType.getDatatype() ) {
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
            if ( !verifyElasticsearchConnection() ) { return false; }
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
            if ( !propertyType.getDatatype().equals( EdmPrimitiveTypeKind.Binary ) ) {
                properties.put( propertyType.getId().toString(), getFieldMapping( propertyType ) );
            }
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
    public boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> entitySetDataModel = Maps.newHashMap();
        entitySetDataModel.put( ENTITY_SET, entitySet );
        entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetDataModel );
            client.prepareIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
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
            Set<AclKey> authorizedAclKeys,
            int start,
            int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder authorizedFilterQuery = new BoolQueryBuilder();
        for ( AclKey aclKey : authorizedAclKeys ) {
            authorizedFilterQuery
                    .should( QueryBuilders.matchQuery( ENTITY_SET + "." + ID, aclKey.get( 0 ).toString() ) );
        }
        authorizedFilterQuery.minimumShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( authorizedFilterQuery );

        if ( optionalSearchTerm.isPresent() ) {
            String searchTerm = optionalSearchTerm.get();
            Map<String, Float> fieldsMap = Maps.newHashMap();
            fieldsMap.put( ENTITY_SET + "." + NAME, Float.valueOf( "1" ) );
            fieldsMap.put( ENTITY_SET + "." + TITLE, Float.valueOf( "1" ) );
            fieldsMap.put( ENTITY_SET + "." + DESCRIPTION, Float.valueOf( "1" ) );

            query.must( QueryBuilders.queryStringQuery( searchTerm ).fields( fieldsMap )
                    .lenient( true ).fuzziness( Fuzziness.AUTO ) );
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
        response.getHits().forEach( hit -> hits.add( hit.getSourceAsMap() ) );
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
                    entitySetId.toString() ).doc( s, XContentType.JSON );
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
            if ( !verifyElasticsearchConnection() ) { return false; }
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
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.admin().indices()
                .delete( new DeleteIndexRequest( getIndexName( entitySetId, syncId ) ) );

        return true;
    }

    @Override
    public List<EntityKey> executeEntitySetDataSearchAcrossIndices(
            Map<UUID, UUID> entitySetAndSyncIds,
            Map<UUID, DelegatedStringSet> fieldSearches,
            int size,
            boolean explain ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return null; }
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
            fieldQuery.minimumShouldMatch( 1 );
            query.should( fieldQuery );
        } );
        query.minimumShouldMatch( 1 );

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
        List<EntityKey> results = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            String[] entitySetIdAndSyncId = hit.getIndex().substring( SECURABLE_OBJECT_INDEX_PREFIX.length() )
                    .split( "_" );
            UUID entitySetId = UUID.fromString( entitySetIdAndSyncId[ 0 ] );
            UUID syncId = UUID.fromString( entitySetIdAndSyncId[ 1 ] );
            EntityKey key = new EntityKey( entitySetId, hit.getId(), syncId );

            results.add( key );
        }
        return results;
    }

    @Override
    public boolean createEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyValues );

            client.prepareIndex( getIndexName( entitySetId, syncId ), getTypeName( entitySetId ), entityId )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating entity data in elasticsearch" );
            return false;
        }

        return true;

    }

    @Override
    public boolean updateEntityData(
            UUID entitySetId,
            UUID syncId,
            String entityId,
            SetMultimap<UUID, Object> propertyValues ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        GetResponse result = client
                .prepareGet( getIndexName( entitySetId, syncId ), getTypeName( entitySetId ), entityId ).get();
        if ( result.isExists() ) {
            result.getSourceAsMap().entrySet().forEach( entry ->
                    propertyValues.putAll( UUID.fromString( entry.getKey() ), (Collection<Object>) entry.getValue() )
            );
        }

        return createEntityData( entitySetId, syncId, entityId, propertyValues );
    }

    @Override
    public boolean deleteEntityData( UUID entitySetId, UUID syncId, String entityId ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
            if ( !verifyElasticsearchConnection() ) { return false; }
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
                    entitySet.getId().toString() ).doc( s, XContentType.JSON );
            client.update( updateRequest ).actionGet();
            return true;
        } catch ( IOException e ) {
            logger.debug( "error updating entity set metadata in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean createOrganization( Organization organization ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        Map<String, Object> organizationObject = Maps.newHashMap();
        organizationObject.put( TITLE, organization.getTitle() );
        organizationObject.put( DESCRIPTION, organization.getDescription() );
        UUID organizationId = organization.getSecurablePrincipal().getId();
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( organizationObject );
            client.prepareIndex( ORGANIZATIONS, ORGANIZATION_TYPE, organizationId.toString() )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error creating organization in elasticsearch" );
        }
        return false;
    }

    @Override
    public boolean deleteOrganization( UUID organizationId ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
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
            Map<String, Object> result = Maps.newHashMap();
            result.put( ID, hit.getId() );
            hits.add( result );
        }
        SearchResult result = new SearchResult( response.getHits().totalHits, hits );
        return result;
    }

    @Override
    public SearchResult executeOrganizationSearch(
            String searchTerm,
            Set<AclKey> authorizedOrganizationIds,
            int start,
            int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder authorizedFilterQuery = new BoolQueryBuilder();
        for ( AclKey aclKey : authorizedOrganizationIds ) {
            authorizedFilterQuery.should( QueryBuilders.matchQuery( "_id", aclKey.get( 0 ).toString() ) );
        }
        authorizedFilterQuery.minimumShouldMatch( 1 );

        BoolQueryBuilder query = new BoolQueryBuilder().must( authorizedFilterQuery )
                .should( QueryBuilders.matchQuery( TITLE, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .should( QueryBuilders.matchQuery( DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
                .minimumShouldMatch( 1 );

        SearchResponse response = client.prepareSearch( ORGANIZATIONS )
                .setTypes( ORGANIZATION_TYPE )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            Map<String, Object> hitMap = hit.getSourceAsMap();
            hitMap.put( "id", hit.getId() );
            hits.add( hitMap );
        }

        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    @Override
    public boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
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
            UpdateRequest updateRequest = new UpdateRequest( ORGANIZATIONS, ORGANIZATION_TYPE, id.toString() )
                    .doc( s, XContentType.JSON );
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
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
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

        BoolQueryBuilder query = QueryBuilders.boolQuery().minimumShouldMatch( 1 );
        searches.forEach( search -> {
            QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery( search.getSearchTerm() )
                    .field( search.getPropertyType().toString(), Float.valueOf( "1" ) ).lenient( true );
            if ( search.getExactMatch() ) {
                query.must( queryString );
                query.minimumShouldMatch( 0 );
            } else { query.should( queryString ); }
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
            Map<String, Object> result = Maps.newHashMap();
            result.put( ID, hit.getId() );
            hits.add( result );
        }
        SearchResult result = new SearchResult( response.getHits().totalHits, hits );
        return result;
    }

    @Override
    public boolean saveEntityTypeToElasticsearch( EntityType entityType ) {
        return saveObjectToElasticsearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityType, entityType.getId().toString() );
    }

    @Override
    public boolean saveAssociationTypeToElasticsearch( AssociationType associationType ) {
        EntityType entityType = associationType.getAssociationEntityType();
        if ( entityType == null ) {
            logger.debug( "An association type must have an entity type present in order to save to elasticsearch" );
            return false;
        }

        return saveObjectToElasticsearch( ASSOCIATION_TYPE_INDEX,
                ASSOCIATION_TYPE,
                associationType,
                entityType.getId().toString() );
    }

    @Override
    public boolean savePropertyTypeToElasticsearch( PropertyType propertyType ) {
        return saveObjectToElasticsearch( PROPERTY_TYPE_INDEX,
                PROPERTY_TYPE,
                propertyType,
                propertyType.getId().toString() );
    }

    @Override
    public boolean saveAppToElasticsearch( App app ) {
        return saveObjectToElasticsearch( APP_INDEX, APP, app, app.getId().toString() );
    }

    @Override
    public boolean saveAppTypeToElasticsearch( AppType appType ) {
        return saveObjectToElasticsearch( APP_TYPE_INDEX, APP_TYPE, appType, appType.getId().toString() );
    }

    @Override
    public boolean deleteEntityType( UUID entityTypeId ) {
        return deleteObjectById( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypeId.toString() );
    }

    @Override
    public boolean deleteAssociationType( UUID associationTypeId ) {
        return deleteObjectById( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, associationTypeId.toString() );
    }

    @Override
    public boolean deletePropertyType( UUID propertyTypeId ) {
        return deleteObjectById( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypeId.toString() );
    }

    @Override
    public boolean deleteApp( UUID appId ) {
        return deleteObjectById( APP_INDEX, APP, appId.toString() );
    }

    @Override
    public boolean deleteAppType( UUID appTypeId ) {
        return deleteObjectById( APP_TYPE_INDEX, APP_TYPE, appTypeId.toString() );
    }

    @Override
    public SearchResult executeEntityTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.EntityType );
        return executeSearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executeAssociationTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.AssociationType );
        return executeSearch( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executePropertyTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.PropertyTypeInEntitySet );
        return executeSearch( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override public SearchResult executeAppSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.App );
        return executeSearch( APP_INDEX, APP, searchTerm, start, maxHits, fieldsMap );
    }

    @Override public SearchResult executeAppTypeSearch( String searchTerm, int start, int maxHits ) {
        Map<String, Float> fieldsMap = getFieldsMap( SecurableObjectType.AppType );
        return executeSearch( APP_TYPE_INDEX, APP_TYPE, searchTerm, start, maxHits, fieldsMap );
    }

    @Override
    public SearchResult executeFQNEntityTypeSearch( String namespace, String name, int start, int maxHits ) {
        return executeFQNSearch( ENTITY_TYPE_INDEX, ENTITY_TYPE, namespace, name, start, maxHits );

    }

    @Override
    public SearchResult executeFQNPropertyTypeSearch( String namespace, String name, int start, int maxHits ) {
        return executeFQNSearch( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, namespace, name, start, maxHits );
    }

    @Override
    public boolean triggerPropertyTypeIndex( List<PropertyType> propertyTypes ) {
        Function<Object, String> idFn = pt -> ( (PropertyType) pt ).getId().toString();
        return triggerIndex( PROPERTY_TYPE_INDEX, PROPERTY_TYPE, propertyTypes, idFn );
    }

    @Override
    public boolean triggerEntityTypeIndex( List<EntityType> entityTypes ) {
        Function<Object, String> idFn = et -> ( (EntityType) et ).getId().toString();
        return triggerIndex( ENTITY_TYPE_INDEX, ENTITY_TYPE, entityTypes, idFn );
    }

    @Override
    public boolean triggerAssociationTypeIndex( List<AssociationType> associationTypes ) {
        Function<Object, String> idFn = at -> ( (AssociationType) at ).getAssociationEntityType().getId().toString();
        return triggerIndex( ASSOCIATION_TYPE_INDEX, ASSOCIATION_TYPE, associationTypes, idFn );
    }

    @Override
    public boolean triggerEntitySetIndex(
            Map<EntitySet, Set<UUID>> entitySets,
            Map<UUID, PropertyType> propertyTypes ) {
        Function<Object, String> idFn = map -> ( (Map<String, EntitySet>) map ).get( ENTITY_SET ).getId().toString();

        List<Map<String, Object>> entitySetMaps = entitySets.entrySet().stream().map( entry -> {
            Map<String, Object> entitySetMap = Maps.newHashMap();
            entitySetMap.put( ENTITY_SET, entry.getKey() );
            entitySetMap.put( PROPERTY_TYPES,
                    entry.getValue().stream().map( id -> propertyTypes.get( id ) ).collect( Collectors.toList() ) );
            return entitySetMap;
        } ).collect( Collectors.toList() );

        return triggerIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetMaps, idFn );
    }

    @Override
    public boolean triggerAppIndex( List<App> apps ) {
        Function<Object, String> idFn = app -> ( (App) app ).getId().toString();
        return triggerIndex( APP_INDEX, APP, apps, idFn );
    }

    @Override
    public boolean triggerAppTypeIndex( List<AppType> appTypes ) {
        Function<Object, String> idFn = at -> ( (AppType) at ).getId().toString();
        return triggerIndex( APP_TYPE_INDEX, APP_TYPE, appTypes, idFn );
    }

    @Override
    public boolean clearAllData() {
        client.admin().indices()
                .delete( new DeleteIndexRequest( SECURABLE_OBJECT_INDEX_PREFIX + "*" ) );
        DeleteByQueryAction.INSTANCE.newRequestBuilder( client )
                .filter( QueryBuilders.matchAllQuery() ).source( ENTITY_SET_DATA_MODEL,
                ENTITY_TYPE_INDEX,
                PROPERTY_TYPE_INDEX,
                ASSOCIATION_TYPE_INDEX,
                ORGANIZATIONS,
                APP_INDEX,
                APP_TYPE_INDEX )
                .get();
        return true;
    }

    @Override
    public double getModelScore( double[][] features ) {
        return ( (MultiLayerNetwork) modelThread.get() ).output( Nd4j.create( features ) ).getDouble( 1 );
    }



    /* HELPERS */

    private boolean saveObjectToElasticsearch( String index, String type, Object obj, String id ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }
        try {
            String s = ObjectMappers.getJsonMapper().writeValueAsString( obj );
            client.prepareIndex( index, type, id )
                    .setSource( s, XContentType.JSON )
                    .execute().actionGet();
            return true;
        } catch ( JsonProcessingException e ) {
            logger.debug( "error saving object to elasticsearch" );
        }
        return false;
    }

    private boolean deleteObjectById( String index, String type, String id ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        client.prepareDelete( index, type, id ).execute().actionGet();
        return true;
    }

    private SearchResult executeSearch(
            String index,
            String type,
            String searchTerm,
            int start,
            int maxHits,
            Map<String, Float> fieldsMap ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        QueryBuilder query = QueryBuilders.queryStringQuery( searchTerm ).fields( fieldsMap ).lenient( true );

        SearchResponse response = client.prepareSearch( index )
                .setTypes( type )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSourceAsMap() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    private SearchResult executeFQNSearch(
            String index,
            String type,
            String namespace,
            String name,
            int start,
            int maxHits ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return new SearchResult( 0, Lists.newArrayList() ); }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.must( QueryBuilders.regexpQuery( TYPE + "." + NAMESPACE, ".*" + namespace + ".*" ) )
                .must( QueryBuilders.regexpQuery( TYPE + "." + NAME, ".*" + name + ".*" ) );

        SearchResponse response = client.prepareSearch( index )
                .setTypes( type )
                .setQuery( query )
                .setFrom( start )
                .setSize( maxHits )
                .execute()
                .actionGet();

        List<Map<String, Object>> hits = Lists.newArrayList();
        for ( SearchHit hit : response.getHits() ) {
            hits.add( hit.getSourceAsMap() );
        }
        return new SearchResult( response.getHits().getTotalHits(), hits );
    }

    private Map<String, Float> getFieldsMap( SecurableObjectType objectType ) {
        float f = Float.valueOf( "1" );
        Map<String, Float> fieldsMap = Maps.newHashMap();

        List<String> fields = Lists.newArrayList( TITLE, DESCRIPTION );
        switch ( objectType ) {
            case AssociationType: {
                fields.add( ENTITY_TYPE_FIELD + "." + TYPE + "." + NAME );
                fields.add( ENTITY_TYPE_FIELD + "." + TYPE + "." + NAMESPACE );
                break;
            }

            case App: {
                fields.add( NAME );
                fields.add( URL );
                break;
            }

            default: {
                fields.add( TYPE + "." + NAME );
                fields.add( TYPE + "." + NAMESPACE );
                break;
            }
        }

        fields.forEach( field -> fieldsMap.put( field, f ) );
        return fieldsMap;
    }

    public boolean triggerIndex(
            String index,
            String type,
            Iterable<? extends Object> objects,
            Function<Object, String> idFn ) {
        try {
            if ( !verifyElasticsearchConnection() ) { return false; }
        } catch ( UnknownHostException e ) {
            logger.debug( "not connected to elasticsearch" );
            e.printStackTrace();
        }

        BoolQueryBuilder deleteQuery = QueryBuilders.boolQuery();
        BulkRequestBuilder bulkRequest = client.prepareBulk();

        objects.forEach( object -> {
            try {
                String id = idFn.apply( object );
                String s = ObjectMappers.getJsonMapper().writeValueAsString( object );
                deleteQuery.mustNot( QueryBuilders.matchQuery( "_id", id ) );
                bulkRequest
                        .add( client.prepareIndex( index, type, id )
                                .setSource( s, XContentType.JSON ) );
            } catch ( JsonProcessingException e ) {
                logger.debug( "Error re-indexing securable object types" );
            }
        } );

        new DeleteByQueryRequestBuilder( client, DeleteByQueryAction.INSTANCE )
                .filter( deleteQuery )
                .source( index )
                .execute()
                .actionGet();

        BulkResponse bulkResponse = bulkRequest.get();
        if ( bulkResponse.hasFailures() ) {
            bulkResponse.forEach( item -> logger
                    .debug( "Failure during attempted re-index: {}", item.getFailureMessage() ) );
        }

        return true;
    }

}
