package com.kryptnostic.kindling.search;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import javax.inject.Inject;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Optional;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.SearchConfiguration;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Sets;

public class ConductorElasticsearchImpl implements ConductorElasticsearchApi {
	
	private Client client;
	private ElasticsearchTransportClientFactory factory;
	private boolean connected = true;
	private String server;
	private String cluster;
	private int port;
	private static final Logger logger = LoggerFactory.getLogger( ConductorElasticsearchImpl.class );
	
	@Inject
	public ConductorElasticsearchImpl( SearchConfiguration config ) throws UnknownHostException {
		init( config );
		client = factory.getClient();
		initializeEntitySetDataModelIndex();
	}

	@Inject
	public ConductorElasticsearchImpl(
			SearchConfiguration config,
			Client someClient ) {
		init( config );
		client = someClient;
		initializeEntitySetDataModelIndex();
	}
	
	private void init( SearchConfiguration config ) {
		server = config.getElasticsearchUrl();
		cluster = config.getElasticsearchCluster();
		port = config.getElasticsearchPort();
		factory = new ElasticsearchTransportClientFactory( server, port, cluster );
	}
	
	@Override
	public Boolean initializeEntitySetDataModelIndex() {
		try {
			if ( !verifyElasticsearchConnection() ) return false;
		} catch (UnknownHostException e) {
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
		.addMapping( ENTITY_SET_TYPE, mapping)
		.addMapping( ACLS, aclMapping )
		.execute().actionGet();
		return true;
	}
	
	@Override
	public Boolean saveEntitySetToElasticsearch( EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal ) {
		try {
			if ( !verifyElasticsearchConnection() ) return false;
		} catch (UnknownHostException e) {
			logger.debug( "not connected to elasticsearch" );
			e.printStackTrace();
		}
	    Map<String, Object> entitySetDataModel = Maps.newHashMap();
	    entitySetDataModel.put( ENTITY_SET, entitySet );
	    entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );
		try {
			String s = ObjectMappers.getJsonMapper().writeValueAsString( entitySetDataModel );
			client.prepareIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() ).setSource( s ).execute().actionGet();
			updateEntitySetPermissions(
					entitySet.getId(),
					principal,
					Sets.newHashSet( Permission.OWNER, Permission.READ, Permission.WRITE, Permission.DISCOVER, Permission.LINK ) );
			return true;
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public List<Map<String, Object>> executeEntitySetDataModelKeywordSearch(
			Optional<String> optionalSearchTerm,
			Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes,
			Set<Principal> principals ) {
		try {
			if ( !verifyElasticsearchConnection() ) return Lists.newArrayList();
		} catch (UnknownHostException e) {
			logger.debug( "not connected to elasticsearch" );
			e.printStackTrace();
		}
		BoolQueryBuilder permissionsQuery = new BoolQueryBuilder();
		for ( Principal principal: principals) {
			BoolQueryBuilder childQuery = new BoolQueryBuilder();
			childQuery.must( QueryBuilders.matchQuery( NAME, principal.getId() ) );
			childQuery.must( QueryBuilders.matchQuery( TYPE, principal.getType().toString() ) );
			childQuery.must( QueryBuilders.regexpQuery( ACLS, ".*" ) );
			String hitName = "acl_" + principal.getType().toString() + "_" + principal.getId();
			permissionsQuery.should( QueryBuilders.hasChildQuery( ACLS, childQuery, ScoreMode.Avg )
					.innerHit( new InnerHitBuilder().setFetchSourceContext( new FetchSourceContext(true, new String[]{ACLS}, null)).setName( hitName ) ) );
		}
		permissionsQuery.minimumNumberShouldMatch( 1 );
		
		BoolQueryBuilder query = new BoolQueryBuilder().must( permissionsQuery );
		
		if ( optionalSearchTerm.isPresent() ) {
			String searchTerm = optionalSearchTerm.get();
			query.should( QueryBuilders.matchQuery( ENTITY_SET + "." + NAME, searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( ENTITY_SET + "." + TITLE, searchTerm).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( ENTITY_SET + "." + DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.minimumNumberShouldMatch( 1 );
		}
		
		if ( optionalEntityType.isPresent() ) {
			UUID eid = optionalEntityType.get();
			query.must( QueryBuilders.matchQuery( ENTITY_SET + "." + ENTITY_TYPE_ID, eid.toString() ) );
		} else if ( optionalPropertyTypes.isPresent() ) {
			Set<UUID> propertyTypes = optionalPropertyTypes.get();
			for ( UUID pid: propertyTypes ) {
				query.must( QueryBuilders.nestedQuery( PROPERTY_TYPES, QueryBuilders.matchQuery( PROPERTY_TYPES + "." + ID, pid.toString() ), ScoreMode.Avg ) );
			}
		}
		SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
				.setTypes( ENTITY_SET_TYPE )
				.setQuery( query )
				.setFetchSource( new String[]{ ENTITY_SET, PROPERTY_TYPES }, null )
				.setFrom( 0 ).setSize( 50 ).setExplain( true )
				.get();
		
		List<Map<String, Object>> hits = Lists.newArrayList();
		for ( SearchHit hit: response.getHits() ) {
			Map<String, Object> match = hit.getSource();
			Set<String> permissions = Sets.newHashSet();
			for( SearchHits innerHits: hit.getInnerHits().values() ) {
				for (SearchHit innerHit: innerHits.getHits() ) {
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
			if ( !verifyElasticsearchConnection() ) return false;
		} catch (UnknownHostException e) {
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
			e.printStackTrace();
		}
		return false;
	}
	
	@Override
	public Boolean updatePropertyTypesInEntitySet( UUID entitySetId, Set<PropertyType> newPropertyTypes ) {
		try {
			if ( !verifyElasticsearchConnection() ) return false;
		} catch (UnknownHostException e) {
			logger.debug( "not connected to elasticsearch" );
			e.printStackTrace();
		}
		
		Map<String, Object> propertyTypes = Maps.newHashMap();
		propertyTypes.put( PROPERTY_TYPES, newPropertyTypes);
		try {
			String s = ObjectMappers.getJsonMapper().writeValueAsString( propertyTypes );
			UpdateRequest updateRequest = new UpdateRequest( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).doc( s );
			client.update( updateRequest ).get();
			return true;
		} catch (IOException | InterruptedException | ExecutionException e) {
			e.printStackTrace();
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
	
	@Scheduled( fixedRate = 1800000 )
	public void verifyRunner() throws UnknownHostException {
		verifyElasticsearchConnection();
	}

	@Override
	public Boolean deleteEntitySet( UUID entitySetId ) {
		try {
			if ( !verifyElasticsearchConnection() ) return false;
		} catch (UnknownHostException e) {
			logger.debug( "not connected to elasticsearch" );
			e.printStackTrace();
		}
		
		Map<String, Object> idField = Maps.newHashMap();
		idField.put( ENTITY_SET_ID, entitySetId.toString() );
		client.prepareDelete( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).get();

		new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE).filter(
				QueryBuilders.boolQuery()
				.must( QueryBuilders.matchQuery( TYPE_FIELD, ACLS ) )
				.must( QueryBuilders.matchQuery(ENTITY_SET_ID, entitySetId.toString() ) ) )
		.source( ENTITY_SET_DATA_MODEL )
		.get();
		
		return true;
	}

}