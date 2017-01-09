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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
	private static final String TYPE = "type";
	private static final String OBJECT = "object";
	private static final String NESTED = "nested";
	private static final String NUM_SHARDS = "index.number_of_shards";
	private static final String NUM_REPLICAS = "index.number_of_replicas";
	
	// entity set field consts
	private static final String ENTITY_SET = "entitySet";
	private static final String PROPERTY_TYPES = "propertyTypes";
	private static final String ROLE_ACLS = "roleAcls";
	private static final String USER_ACLS = "userAcls";
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
		Map<String, Object> properties = Maps.newHashMap();
		properties.put( PROPERTY_TYPES, Maps.newHashMap()
				.put( TYPE, NESTED ) );
		properties.put( ROLE_ACLS, Maps.newHashMap()
				.put( TYPE, OBJECT ) );
		properties.put( USER_ACLS, Maps.newHashMap()
				.put( TYPE, OBJECT ) );
		properties.put( ENTITY_SET, Maps.newHashMap()
				.put( TYPE, OBJECT ) );
		Map<String, Object> mapping = Maps.newHashMap();
		mapping.put( ENTITY_SET_TYPE, Maps.newHashMap()
				.put( ES_PROPERTIES, properties ) );

		client.admin().indices().prepareCreate( ENTITY_SET_DATA_MODEL )
		.setSettings( Settings.builder()
				.put( NUM_SHARDS, 3 )
				.put( NUM_REPLICAS, 2 ) )
		.addMapping( ENTITY_SET_TYPE, mapping)
		.get();
	}
	
	public void saveEntitySetToElasticsearch( EntitySet entitySet, Set<PropertyType> propertyTypes ) {
	//	logger.debug("\n\n\n\n" + entitySet.toString() + "\n\n\n\n\n");
	//	XContentBuilder builder;
//			builder = XContentFactory.jsonBuilder().startObject()
//					.field( "id", entitySet.getId() )
//					.field( "typename", entitySet.getType().getFullQualifiedNameAsString() )
//					.field( "name", entitySet.getName() )
//					.field( "title", entitySet.getTitle() )
//					.field( "description", entitySet.getDescription() );
//	        builder.startArray( "propertyTypes" );
//	        for ( PropertyType propertyType: propertyTypes ) {
//	        	builder.value( propertyType.getType().getFullQualifiedNameAsString() );
//	        }
//	        builder.endArray();
//	        builder.endObject();
//	        String json = builder.string();
//	        logger.debug(json);
			//Map<String, Object> permissions = Maps.newHashMap();

		//			Map<String, List<String>> rolePermissions = Maps.newHashMap();
//			List<String> ps = Lists.newArrayList();
//			ps.add("read");
//			rolePermissions.put( "user", ps );
//			Map<String, List<String>> userPermissions = Maps.newHashMap();
//			userPermissions.put( "katherine", ps );
	        Map<String, Object> entitySetDataModel = Maps.newHashMap();
	        entitySetDataModel.put( ENTITY_SET, entitySet );
	        entitySetDataModel.put( PROPERTY_TYPES, propertyTypes );
	        entitySetDataModel.put( ROLE_ACLS, Maps.newHashMap() );
	        entitySetDataModel.put( USER_ACLS, Maps.newHashMap() );
	        
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
			String typePath = ( principal.getType() == PrincipalType.USER ) ? USER_ACLS : ROLE_ACLS;
			permissionsQuery.should( QueryBuilders.regexpQuery( typePath + "." + principal.getId(), ".*" ) );
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

		SearchResponse response = client.prepareSearch( ENTITY_SET_DATA_MODEL )
				.setTypes( ENTITY_SET_TYPE )
			//	.setQuery( QueryBuilders.matchQuery( "_all", query ).fuzziness( Fuzziness.AUTO ) )
				.setQuery( query )
				//.setFetchSource( new String[]{ ENTITY_SET, PROPERTY_TYPES }, null )
				.setFrom( 0 ).setSize( 50 ).setExplain( true )
				.get();
		logger.debug( response.getHits().getAt( 0 ).getSourceAsString() );
	//	logger.debug( response.getHits().getAt( 0 ).getInnerHits().toString() );
	//	List<String> hits = Lists.newArrayList();
	//	for ( SearchHit hit: response.getHits() ) {
	//		hits.add( hit.getSourceAsString() );
	//	}
	//	logger.debug( hits.toString() );
	}
	
	public void updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
		String typeField = (principal.getType() == PrincipalType.ROLE) ? ROLE_ACLS : USER_ACLS;
		ObjectMapper mapper = new ObjectMapper();
		mapper.registerModule( new GuavaModule() );
		mapper.registerModule( new JodaModule() );
		Map<String, Object> permissionsMap = Maps.newHashMap();
		try {
			permissionsMap.put( principal.getId(), permissions );
			Map<String, Object> newPermissions = Maps.newHashMap();
			newPermissions.put( typeField, permissionsMap );
			
			String s = mapper.writeValueAsString( newPermissions );
			UpdateRequest updateRequest = new UpdateRequest( ENTITY_SET_DATA_MODEL, ENTITY_SET_TYPE, entitySetId.toString() ).doc( s );
			client.update( updateRequest ).get();

		} catch ( InterruptedException | ExecutionException | IOException e) {
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
