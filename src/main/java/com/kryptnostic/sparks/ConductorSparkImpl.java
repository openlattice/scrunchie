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

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.authorization.Permission;
import com.dataloom.authorization.Principal;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.organization.Organization;
import com.dataloom.search.requests.SearchResult;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.base.Optional;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.EdmManager;

public class ConductorSparkImpl implements ConductorSparkApi, Serializable {
    private static final long               serialVersionUID = 825467486008335571L;
    private static final Logger             logger           = LoggerFactory
            .getLogger( ConductorSparkImpl.class );
    private static final String             LEFTOUTER        = "leftouter";
    private final SecureRandom              random           = new SecureRandom();
    private static final String             CACHE_KEYSPACE   = "cache";

    private final SparkSession              sparkSession;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final String                    keyspace;
    private final EdmManager                dataModelService;
    private final ConductorElasticsearchApi elasticsearchApi;

    // private final ConcurrentMap<FullQualifiedName, Dataset<Row>> entityDataframeMap;
    // private final ConcurrentMap<FullQualifiedName, Dataset<Row>> propertyDataframeMap;
    // private final ConcurrentMap<String, Dataset<Row>> entitySetDataframes;

    @Inject
    public ConductorSparkImpl(
            String keyspace,
            SparkSession sparkSession,
            SparkContextJavaFunctions cassandraJavaContext,
            EdmManager dataModelService,
            HazelcastInstance hazelcastInstance,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.sparkSession = sparkSession;
        this.cassandraJavaContext = cassandraJavaContext;
        this.keyspace = keyspace;
        this.dataModelService = dataModelService;

        this.sparkSession.sql( "set spark.sql.caseSensitive=false" );
        // this.entityDataframeMap = Maps.newConcurrentMap();// hazelcastInstance.getMap(
        // // HazelcastNames.Maps.ENTITY_DATAFRAMES );
        // this.propertyDataframeMap = Maps.newConcurrentMap(); // hazelcastInstance.getMap(
        // // HazelcastNames.Maps.PROPERTY_DATAFRAMES );
        // this.entitySetDataframes = Maps.newConcurrentMap();// hazelcastInstance.getMap(
        // // HazelcastNames.Maps.ENTITY_SET_DATAFRAMES );
        // //prepareDataframe();
        this.elasticsearchApi = elasticsearchApi;
    }

    // private void prepareDataframe() {
    // dataModelService.getEntityTypes()
    // .forEach( entityType -> {
    // String entityTableName = cassandraTableManager.getTablenameForEntityType( entityType );
    // Dataset<Row> entityDf = sparkSession
    // .read()
    // .format( "org.apache.spark.sql.cassandra" )
    // .option( "table", entityTableName.toLowerCase() )
    // .option( "keyspace", keyspace )
    // .load();
    // entityDataframeMap.put( entityType.getFullQualifiedName(), entityDf );
    //
    // entityType.getProperties().forEach( fqn -> {
    // String propertyTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( fqn );
    // Dataset<Row> propertyDf = sparkSession
    // .read()
    // .format( "org.apache.spark.sql.cassandra" )
    // .option( "table", propertyTableName )
    // .option( "keyspace", keyspace )
    // .load();
    // propertyDataframeMap.put( fqn, propertyDf );
    // } );
    // } );
    //
    // }

//    public void count() {
//        Dataset<Row> entityDf = sparkSession
//         .read()
//         .format( "org.apache.spark.sql.cassandra" )
//         .option( "table", Table.AUDIT_EVENTS.getName().toLowerCase() )
//         .option( "keyspace", keyspace )
//         .load();
//    }
    
    @Override
    public QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName ) {
        EntityType entityType = dataModelService.getEntityType( entityFqn );
        List<PropertyType> propertyTypes = entityType.getProperties().stream()
                .map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect( Collectors.toList() );

        return getAllEntitiesOfEntitySet( entityFqn, entitySetName, propertyTypes );
    }

    @Override
    public QueryResult getAllEntitiesOfEntitySet(
            FullQualifiedName entityFqn,
            String entitySetName,
            List<PropertyType> authorizedProperties ) {

        EntityType entityType = dataModelService.getEntityType( entityFqn );
        /*
         * authorizedProperties.forEach( property -> { FullQualifiedName fqn = property.getFullQualifiedName(); if (
         * propertyDataframeMap.get( fqn ) == null ) { Dataset<Row> propertyDf = sparkSession .read() .format(
         * "org.apache.spark.sql.cassandra" ) .option( "table",
         * cassandraTableManager.getTablenameForPropertyValuesOfType( fqn ) ) .option( "keyspace", keyspace ) .load();
         * propertyDataframeMap.put( fqn, propertyDf ); } } );
         */

        // Dataset<Row> entityDf = entityDataframeMap.get( entityFqn );

        // if ( entityDf == null ) {
        // entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
        // .option( "keyspace", keyspace )
        // .load();
        // entityDataframeMap.put( entityFqn, entityDf );
        // }
        // Dataset<Row> entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", Table.SET. )
        // .option( "keyspace", keyspace )
        // .load();
        // List<String> columns = authorizedProperties.stream()
        // .map( pt -> Queries.fqnToColumnName( pt.getType() ) )
        // // List<String> columns = authorizedProperties.stream().map( pt -> pt.getTypename() )
        // .collect( Collectors.toList() );
        // Preconditions.checkState( columns.size() > 0, "Must have access to at least one column." );
        //
        // entityDf = entityDf.select( CommonColumns.ENTITYID.cql(), columns.toArray( new String[] {} ) )
        // .where(
        // "array_contains( "
        // + CommonColumns.ENTITY_SETS.cql()
        // + ", "
        // + "'" + entitySetName + "'"
        // + ")"
        // );

        /*
         * entityDf = sparkSession .sql( "select " + CommonColumns.ENTITYID.cql() +
         * " from entityDf where array_contains( " + CommonColumns.ENTITY_SETS.cql() + ", '" + entitySetName + "')" );
         * List<Dataset<Row>> propertyDataFrames = authorizedProperties.stream() .map( property -> propertyDataframeMap
         * .get( property.getFullQualifiedName() ) .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() ) )
         * .collect( Collectors.toList() ); for ( Dataset<Row> rdf : propertyDataFrames ) { entityDf = entityDf.join(
         * rdf, scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
         * .toList(), LEFTOUTER ); }
         */
        // String tableName = cacheToCassandra( entityDf, authorizedProperties );

        // return new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() );
        return null;
    }

    /**
     * Return QueryResult of UUID's ONLY of all entities linking a Look Up Entities Request.
     *
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's linking the lookup request
     */
    @Override
    public QueryResult getFilterEntities( LookupEntitiesRequest request ) {
        // Get set of JavaRDD of UUIDs linking the property value for each property type
        // final Map<FullQualifiedName, QueryResult> dfs = Maps.newConcurrentMap();
        //
        // request.getEntityTypes().forEach( entityFqn -> {
        // Dataset<Row> entityDf = entityDataframeMap.get( entityFqn );

        // if ( entityDf == null ) {
        // entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", cassandraTableManager.getTablenameForEntityType( entityFqn ) )
        // .option( "keyspace", keyspace )
        // .load();
        // entityDataframeMap.put( entityFqn, entityDf );
        // }
        // Dataset<Row> entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", cassandraTableManager.getTablenameForEntityType( entityFqn ) )
        // .option( "keyspace", keyspace )
        // .load();
        //
        // for ( Entry<FullQualifiedName, Object> e : request.getPropertyTypeToValueMap().entrySet() ) {
        // entityDf = entityDf
        // .filter( entityDf.apply( Queries.fqnToColumnName( e.getKey() ) )
        // .equalTo( e.getValue() instanceof UUID ? e.getValue().toString() : e.getValue() ) );
        // }
        // String tableName = cacheToCassandra( entityDf,
        // dataModelService.getEntityType( entityFqn ).getProperties().stream()
        // .map( dataModelService::getPropertyType ).collect( Collectors.toList() ) );
        // dfs.put( entityFqn,
        // new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() ) );
        // entityDf.createOrReplaceTempView( "entityDf" );
        //
        // } );
        // return dfs.values().iterator().next();
        return null;
        // Set<JavaRDD<UUID>> resultsMatchingPropertyValues = request.getPropertyTypeToValueMap().entrySet()
        // .parallelStream()
        // .map( ptv -> getEntityIds( request.getUserId(),
        // cassandraTableManager.getTablenameForPropertyValuesOfType( ptv.getKey() ),
        // ptv.getValue() ) )
        // .collect( Collectors.toSet() );
        //
        // // Take intersection to get the JavaRDD of UUIDs linking all the property type values, but before filtering
        // // Entity Types
        // // TODO: repartitionbyCassandraReplica is not done, which means that intersection is potentially extremely
        // slow.
        // JavaRDD<UUID> resultsBeforeFilteringEntityTypes = resultsMatchingPropertyValues.stream()
        // .reduce( ( leftRDD, rightRDD ) -> leftRDD.intersection( rightRDD ) )
        // .get();
        //
        // // Get the RDD of UUIDs linking all the property type values, after filtering Entity Types
        // // TODO: once Hristo's entity type to entity id table is done, maybe faster to use that rather than do
        // multiple
        // // joinWithCassandraTable
        // // Looks like JavaSparkContext is not injected anymore.
        // JavaRDD<UUID> resultsAfterFilteringEntityTypes = ( new JavaSparkContext( sparkSession.sparkContext() ) )
        // .emptyRDD();
        //
        // if ( !resultsBeforeFilteringEntityTypes.isEmpty() ) {
        // resultsAfterFilteringEntityTypes = request.getEntityTypes()
        // .stream()
        // .map( typeFQN -> cassandraTableManager.getTablenameForEntityType( typeFQN ) )
        // .map( typeTablename -> CassandraJavaUtil.javaFunctions( resultsBeforeFilteringEntityTypes )
        // .joinWithCassandraTable( keyspace,
        // typeTablename,
        // CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ),
        // CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ),
        // CassandraJavaUtil.mapColumnTo( UUID.class ),
        // // RowWriter not really necessary - should not be invoked during lazy evaluation.
        // new RowWriterFactory<UUID>() {
        //
        // @Override
        // public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
        // return new RowWriterForUUID();
        // }
        // } )
        // .keys() )
        // .reduce( ( leftRDD, rightRDD ) -> leftRDD.union( rightRDD ) )
        // .get();
        // }
        //
        // // Write to QueryResult
        // // Build Temp Table, using Yao's initializeTempTable function
        // // Initialize Temp Table
        // String cacheTable = initializeTempTable(
        // ImmutableList.of( CommonColumns.ENTITYID.cql() ),
        // ImmutableList.of( DataType.uuid() ) );
        // // Save RDD of entityID's to Cassandra.
        // CassandraJavaUtil.javaFunctions( resultsAfterFilteringEntityTypes )
        // .writerBuilder( CACHE_KEYSPACE,
        // cacheTable,
        // // toModify
        // new RowWriterFactory<UUID>() {
        //
        // @Override
        // public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
        // return new RowWriterForUUID();
        // }
        //
        // } )
        // .saveToCassandra();
        //
        // // Return Query Result pointing to the temp table.
        // return new QueryResult( CACHE_KEYSPACE, cacheTable, UUID.randomUUID(), UUID.randomUUID().toString() );
    }

    private JavaRDD<UUID> getEntityIds( UUID userId, String table, Object value ) {
        return cassandraJavaContext.cassandraTable( keyspace, table, CassandraJavaUtil.mapColumnTo( UUID.class ) )
                .select( CommonColumns.ENTITYID.cql() )
                .where( "value = ?", value )
                .distinct();
    }

    @Override
    public QueryResult getAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn );
        List<PropertyType> propertyTypes = entityType.getProperties().stream()
                .map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect( Collectors.toList() );

        return getAllEntitiesOfType( entityTypeFqn, propertyTypes );
    }

    @Override
    public QueryResult getAllEntitiesOfType(
            FullQualifiedName entityTypeFqn,
            List<PropertyType> authorizedProperties ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn );

        /*
         * authorizedProperties.forEach( property -> { FullQualifiedName fqn = property.getFullQualifiedName(); if (
         * propertyDataframeMap.get( fqn ) == null ) { Dataset<Row> propertyDf = sparkSession .read() .format(
         * "org.apache.spark.sql.cassandra" ) .option( "table",
         * cassandraTableManager.getTablenameForPropertyValuesOfType( fqn ) ) .option( "keyspace", keyspace ) .load();
         * propertyDataframeMap.put( fqn, propertyDf ); } } );
         */

        // Dataset<Row> entityDf = entityDataframeMap.get( entityTypeFqn );
        //
        // if ( entityDf == null ) {
        // entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
        // .option( "keyspace", keyspace )
        // .load();
        // entityDataframeMap.put( entityTypeFqn, entityDf );
        // }
        //
        // Dataset<Row> entityDf = sparkSession
        // .read()
        // .format( "org.apache.spark.sql.cassandra" )
        // .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
        // .option( "keyspace", keyspace )
        // .load();
        //
        // List<String> columns = authorizedProperties.stream()
        // .map( pt -> Queries.fqnToColumnName( pt.getType() ) )
        // // List<String> columns = authorizedProperties.stream().map( pt -> pt.getTypename() )
        // .collect( Collectors.toList() );
        // Preconditions.checkState( columns.size() > 0, "Must have access to at least one column." );
        //
        // entityDf = entityDf.select( CommonColumns.ENTITYID.cql(), columns.toArray( new String[] {} ) );

        /*
         * entityDf = entityDf.select( CommonColumns.ENTITYID.cql() ); List<Dataset<Row>> propertyDataFrames =
         * authorizedProperties.stream() .map( property -> propertyDataframeMap .get( property.getFullQualifiedName() )
         * .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() ) ) .collect( Collectors.toList() ); for (
         * Dataset<Row> rdf : propertyDataFrames ) { entityDf = entityDf.join( rdf,
         * scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) ) .toList(),
         * LEFTOUTER ); }
         */
        return null;
        // String tableName = cacheToCassandra( entityDf, authorizedProperties );
        //
        // return new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() );
    }

    private String cacheToCassandra( Dataset<Row> df, List<PropertyType> propertyTypes ) {
        List<String> columnNames = propertyTypes.stream()
                .map( pt -> "value_" + pt.getType().getFullQualifiedNameAsString() )
                .collect( Collectors.toList() );
        List<DataType> propertyDataTypes = propertyTypes.stream()
                .map( pt -> CassandraEdmMapping.getCassandraType( pt.getDatatype() ) )
                .collect( Collectors.toList() );

        columnNames.add( 0, "entityid" );
        propertyDataTypes.add( 0, DataType.uuid() );

        String tableName = initializeTempTable(
                columnNames,
                propertyDataTypes );

        CassandraJavaUtil.javaFunctions( df.toJavaRDD() )
                .writerBuilder( "cache",
                        tableName,
                        ( table, selectedColumns ) -> new CacheTableRowWriter( columnNames ) )
                .saveToCassandra();
        return tableName;
    }

    private String initializeTempTable( List<String> columnNames, List<DataType> dataTypes ) {
        String tableName = getValidCacheTableName();
        String query = new CacheTableBuilder( tableName ).columns( columnNames, dataTypes ).buildQuery();

        CassandraConnector cassandraConnector = CassandraConnector.apply( sparkSession.sparkContext().conf() );
        try ( Session session = cassandraConnector.openSession() ) {
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS cache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" );
            session.execute( query );
        }
        return tableName;
    }

    // TODO: move to Util and redesign
    String getValidCacheTableName() {
        String rdm = new BigInteger( 130, random ).toString( 32 );
        return "cache_" + rdm;
    }

	@Override
	public Boolean submitEntitySetToElasticsearch(EntitySet entitySet, List<PropertyType> propertyTypes, Principal principal) {
		return elasticsearchApi.saveEntitySetToElasticsearch( entitySet, propertyTypes, principal );
	}

	@Override
	public List<Map<String, Object>> executeElasticsearchMetadataQuery(Optional<String> optionalQuery, Optional<UUID> optionalEntityType,
			Optional<Set<UUID>> optionalPropertyTypes, Set<Principal> principals) {
		return elasticsearchApi.executeEntitySetDataModelKeywordSearch( optionalQuery, optionalEntityType, optionalPropertyTypes, principals );
	}
	
	@Override
	public Boolean updateEntitySetPermissions( UUID entitySetId, Principal principal, Set<Permission> permissions ) {
		return elasticsearchApi.updateEntitySetPermissions( entitySetId, principal, permissions );
	}

	@Override
	public Boolean deleteEntitySet( UUID entitySetId ) {
		return elasticsearchApi.deleteEntitySet( entitySetId );
	}
	
	@Override
	public List<Entity> executeEntitySetDataSearchAcrossIndices( Set<UUID> entitySetIds, Map<UUID, Set<String>> fieldSearches, int size, boolean explain ) {
	    return elasticsearchApi.executeEntitySetDataSearchAcrossIndices( entitySetIds, fieldSearches, size, explain );
	}


    @Override
    public Boolean createEntityData( UUID entitySetId, String entityId, Map<UUID, Object> propertyValues ) {
        return elasticsearchApi.createEntityData( entitySetId, entityId, propertyValues );
    }

    @Override
    public SearchResult executeEntitySetDataSearch( UUID entitySetId, String searchTerm, int start, int maxHits, Set<UUID> authorizedPropertyTypes ) {
        return elasticsearchApi.executeEntitySetDataSearch( entitySetId, searchTerm, start, maxHits, authorizedPropertyTypes );
    }
    
    @Override
    public Boolean createOrganization( Organization organization, Principal principal ) {
        return elasticsearchApi.createOrganization( organization, principal );
    }

    @Override
    public List<Map<String, Object>> executeOrganizationKeywordSearch( String searchTerm, Set<Principal> principals ) {
        return elasticsearchApi.executeOrganizationSearch( searchTerm, principals );
    }

    @Override
    public Boolean updateOrganization( UUID id, Optional<String> optionalTitle, Optional<String> optionalDescription ) {
        return elasticsearchApi.updateOrganization( id, optionalTitle, optionalDescription );
    }

    @Override
    public Boolean deleteOrganization( UUID organizationId ) {
        return elasticsearchApi.deleteOrganization( organizationId );
    }

    @Override
    public Boolean updateOrganizationPermissions( UUID organizationId, Principal principal, Set<Permission> permissions ) {
        return elasticsearchApi.updateOrganizationPermissions( organizationId, principal, permissions );
    }
    
    @Override
    public Boolean updateEntitySetMetadata( EntitySet entitySet ) {
        return elasticsearchApi.updateEntitySetMetadata( entitySet );
    }

    @Override
    public Boolean updatePropertyTypesInEntitySet( UUID entitySetId, List<PropertyType> newPropertyTypes ) {
        return elasticsearchApi.updatePropertyTypesInEntitySet( entitySetId, newPropertyTypes );
    }

    @Override
    public Void clustering( UUID linkedEntitySetId ){
        throw new NotImplementedException("Ho Chung");
//        clusterer.cluster();
//        return null;
    }
}
