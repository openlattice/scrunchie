package com.kryptnostic.kindling.search;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import com.dataloom.authorization.requests.Permission;
import com.dataloom.authorization.requests.Principal;
import com.dataloom.authorization.requests.PrincipalType;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Optional;

import jersey.repackaged.com.google.common.collect.Lists;

public class KindlingElasticsearchHandler {
	
	// index setup consts
	private static final String ENTITY_SET_DATA_MODEL = "entity_set_data_model";
	private static final String ENTITY_SET_TYPE = "entity_set";
	private static final String ES_PROPERTIES = "properties";
	private static final String PARENT = "parent";
	private static final String TYPE = "type";
	private static final String OBJECT = "object";
	private static final String NESTED = "nested";
	private static final String KEYWORD = "keyword";
	private static final String NUM_SHARDS = "index.number_of_shards";
	private static final String NUM_REPLICAS = "index.number_of_replicas";
	
	// entity set field consts
	private static final String ENTITY_SET = "entitySet";
	private static final String PROPERTY_TYPES = "propertyTypes";
	private static final String ACLS = "acls";
	private static final String NAME = "name";
	private static final String TITLE = "title";
	private static final String DESCRIPTION = "description";
	private static final String ENTITY_TYPE_ID = "entityTypeId";
	private static final String ID = "id";
	
	
	private Client client;
	private KindlingTransportClientFactory factory;
	private boolean connected = true;
	private String server;
	private String cluster;
	private static final Logger logger = LoggerFactory.getLogger( KindlingElasticsearchHandler.class );
	
	public KindlingElasticsearchHandler( KindlingConfiguration config ) throws UnknownHostException {
		init( config );
		client = factory.getClient();
	}
	
	public KindlingElasticsearchHandler(
			KindlingConfiguration config,
			Client someClient ) {
		init( config );
		client = someClient;
	}
	
	private void init( KindlingConfiguration config ) {
		server = config.getElasticsearchUrl().get();
		cluster = config.getElasticsearchCluster().get();
		factory = new KindlingTransportClientFactory( server, 9300, false, cluster );
	}
	
	public void initializeEntitySetDataModelIndex() {
		
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
		aclData.put( ES_PROPERTIES, aclProperties );
		aclData.put( PARENT, aclParent );
		aclMapping.put( ACLS, aclData );
		
		client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
		.setSettings( Settings.builder()
				.put( NUM_SHARDS, 3 )
				.put( NUM_REPLICAS, 2 ) )
		.addMapping( ENTITY_SET_TYPE, mapping)
		.addMapping( ACLS, aclMapping )
		.get();
	}
	
	public void saveEntitySetToElasticsearch( EntitySet entitySet, Set<PropertyType> propertyTypes ) {
	        Map<String, Object> entitySetDataModel = Maps.newHashMap();
	        entitySetDataModel.put( ENTITY_SET, entitySet );
	        entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );
	        
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule( new GuavaModule() );
			mapper.registerModule( new JodaModule() );
			try {
				String s = mapper.writeValueAsString( entitySetDataModel );
				client.prepareIndex( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySet.getId().toString() ).setSource( s ).execute().actionGet();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
	}
		
	public void executeEntitySetDataModelKeywordSearch(
			Set<Principal> principals,
			String searchTerm,
			Optional<UUID> optionalEntityType,
			Optional<List<UUID>> optionalPropertyTypes ) {
		
		
		try {
			if ( !verifyElasticsearchConnection() ) return;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		BoolQueryBuilder permissionsQuery = new BoolQueryBuilder();
		for ( Principal principal: principals) {
			BoolQueryBuilder childQuery = new BoolQueryBuilder();
			childQuery.must( QueryBuilders.matchQuery( NAME, principal.getId() ) );
			childQuery.must( QueryBuilders.matchQuery( TYPE, principal.getType().toString() ) );
			childQuery.must( QueryBuilders.regexpQuery( ACLS, ".*" ) );
			permissionsQuery.should( QueryBuilders.hasChildQuery( ACLS, childQuery, org.apache.lucene.search.join.ScoreMode.Avg)
					.innerHit( new InnerHitBuilder().setDocValueFields( Lists.newArrayList( ACLS )) ) );
		}
		permissionsQuery.minimumNumberShouldMatch( 1 );
		
		BoolQueryBuilder query = new BoolQueryBuilder()
				.must( permissionsQuery )
				.should( QueryBuilders.matchQuery( ENTITY_SET + "." + NAME, searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( ENTITY_SET + "." + TITLE, searchTerm).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( ENTITY_SET + "." + DESCRIPTION, searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.minimumNumberShouldMatch( 1 );
		if ( optionalEntityType.isPresent() ) {
			UUID eid = optionalEntityType.get();
			query.must( QueryBuilders.matchQuery( ENTITY_SET + "." + ENTITY_TYPE_ID, eid.toString() ) );
		} else if ( optionalPropertyTypes.isPresent() ) {
			List<UUID> propertyTypes = optionalPropertyTypes.get();
			for ( UUID pid: propertyTypes ) {
				query.must( QueryBuilders.matchQuery( PROPERTY_TYPES + "." + ID, pid.toString() ) );
			}
		}
		logger.debug("A");
		SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
				.setTypes( ENTITY_SET_TYPE )
			//	.setQuery( QueryBuilders.matchQuery( "_all", query ).fuzziness( Fuzziness.AUTO ) )
				.setQuery( query )
				.setFetchSource( new String[]{ ENTITY_SET, PROPERTY_TYPES }, null )
				.setFrom( 0 ).setSize( 50 ).setExplain( true )
				.get();
		logger.debug( response.toString() );
	//	logger.debug( response.getHits().getAt( 0 ).getInnerHits().toString() );
	//	List<String> hits = Lists.newArrayList();
		for ( SearchHit hit: response.getHits() ) {
	//		logger.debug( hit.getInnerHits().get( "acls").getAt(0).getSourceAsString() );
			logger.debug( String.valueOf(hit.getInnerHits().size() ) );
//			logger.debug("all inner hits.....");
//			logger.debug( hit.getInnerHits().toString());
//			logger.debug( hit.getInnerHits().get( "acls").toString());
//	//		logger.debug( hit.getInnerHits().get( "acls").);
//			for ( SearchHit innerHit: hit.getInnerHits().get( "acls" ) ) {
//			//for (String key: hit.getInnerHits().keySet() ) {
//				logger.debug("INNER HIT");
//				logger.debug( innerHit.getSourceAsString());
//				//logger.debug( key);
//				//for (SearchHit acl: hit.getInnerHits().get(key) ) {
//					//logger.debug( acl.getSource().get("acls").toString() );
//				//}
//			//	logger.debug( hit.getInnerHits().get( key ).toString() );
//			}
		//	logger.debug( hit.getInnerHits().toString() );
		//	hits.add( hit.getSourceAsString() );
		}
	//	logger.debug( hits.toString() );
	}
	
	public void updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
        Map<String, Object> acl = Maps.newHashMap();
        acl.put( ACLS, permissions );
        acl.put( TYPE, principal.getType().toString() );
        acl.put( NAME, principal.getId() );
        
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule( new GuavaModule() );
		mapper.registerModule( new JodaModule() );
		try {
			String s = mapper.writeValueAsString( acl );
			String id = entitySetId.toString() + "_" + principal.getType().toString() + "_" + principal.getId();
			client.prepareIndex( ENTITY_SET_DATA_MODEL, ACLS, id ).setParent( entitySetId.toString() ).setSource( s ).execute().actionGet();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	
	public boolean verifyElasticsearchConnection() throws UnknownHostException {
		if ( connected ) {
			if ( !factory.verifyConnection( client ) ) {
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

}
