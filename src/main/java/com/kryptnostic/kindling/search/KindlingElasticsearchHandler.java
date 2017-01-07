package com.kryptnostic.kindling.search;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;
import org.springframework.scheduling.annotation.Scheduled;

import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.PropertyType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Optional;

public class KindlingElasticsearchHandler {
	
	private Client client;
	private KindlingTransportClientFactory factory;
	private boolean connected = true;
	private String server;
	private String cluster;
	private static final Logger logger = LoggerFactory.getLogger( KindlingElasticsearchHandler.class );
	
	public KindlingElasticsearchHandler( KindlingConfiguration config ) throws UnknownHostException {
		init( config  );
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
		Map<String, Object> mapping = Maps.newHashMap();
		mapping.put( "entity_set", Maps.newHashMap()
				.put("properties", Maps.newHashMap()
						.put( "propertyTypes", Maps.newHashMap()
								.put( "type", "nested" ) ) ) );

		client.admin().indices().prepareCreate( "entity_set_data_model" )
		.setSettings( Settings.builder()
				.put( "index.number_of_shards", 3)
				.put( "index.number_of_replicas", 2 ) )
		.addMapping( "entity_set", mapping)
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

	        
	        Map<String, Object> entitySetDataModel = Maps.newHashMap();
	        entitySetDataModel.put("entitySet", entitySet);
	        entitySetDataModel.put("propertyTypes", propertyTypes);
	        
			ObjectMapper mapper = new ObjectMapper();
			mapper.registerModule( new GuavaModule() );
			mapper.registerModule( new JodaModule() );
			try {
				String s = mapper.writeValueAsString( entitySetDataModel );
				client.prepareIndex( "entity_set_data_model", "entity_set", entitySet.getName() ).setSource( s ).execute().actionGet();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
	}
	
	public void executeEntitySetDataModelKeywordSearch(
			String searchTerm,
			Optional<FullQualifiedName> optionalEntityType,
			Optional<List<FullQualifiedName>> optionalPropertyTypes ) {
		
		
		try {
			if ( !verifyElasticsearchConnection() ) return;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		BoolQueryBuilder query = new BoolQueryBuilder()
				.should( QueryBuilders.matchQuery( "entitySet.name", searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( "entitySet.title", searchTerm).fuzziness( Fuzziness.AUTO ) )
				.should( QueryBuilders.matchQuery( "entitySet.description", searchTerm ).fuzziness( Fuzziness.AUTO ) )
				.minimumNumberShouldMatch( 1 );
		if ( optionalEntityType.isPresent() ) {
			FullQualifiedName entityType = optionalEntityType.get();
			query.must( QueryBuilders.matchQuery( "entitySet.type.namespace", entityType.getNamespace() ) );
			query.must( QueryBuilders.matchQuery( "entitySet.type.name", entityType.getName() ) );
		}
		if ( optionalPropertyTypes.isPresent() ) {
			List<FullQualifiedName> propertyTypes = optionalPropertyTypes.get();
			for ( FullQualifiedName fqn: propertyTypes ) {
				query.must( QueryBuilders.matchQuery( "propertyTypes.type.namespace", fqn.getNamespace() ) )
				.must( QueryBuilders.matchQuery( "propertyTypes.type.name", fqn.getName() ) );
			}
		}
			
		SearchResponse response = client.prepareSearch( "entity_set_data_model" )
				.setTypes( "entity_set" )
			//	.setQuery( QueryBuilders.matchQuery( "_all", query ).fuzziness( Fuzziness.AUTO ) )
				.setQuery( query )
				.setFrom( 0 ).setSize( 50 ).setExplain( true )
				.get();
		logger.debug( response.toString() );
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
