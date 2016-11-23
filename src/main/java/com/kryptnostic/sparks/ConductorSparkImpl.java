package com.kryptnostic.sparks;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.data.requests.LookupEntitiesRequest;
import com.dataloom.edm.internal.EntityType;
import com.dataloom.edm.internal.PropertyType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.Maps;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.cassandra.Queries;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;

public class ConductorSparkImpl implements ConductorSparkApi, Serializable {
    private static final long         serialVersionUID = 825467486008335571L;
    private static final Logger       logger           = LoggerFactory
            .getLogger( ConductorSparkImpl.class );
    private static final String       LEFTOUTER        = "leftouter";
    private final        SecureRandom random           = new SecureRandom();
    private static final String       CACHE_KEYSPACE   = "cache";

    private final SparkSession              sparkSession;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final SparkAuthorizationManager authzManager;
    private final String                    keyspace;
    private final CassandraTableManager     cassandraTableManager;
    private final EdmManager                dataModelService;

//    private final ConcurrentMap<FullQualifiedName, Dataset<Row>> entityDataframeMap;
//    private final ConcurrentMap<FullQualifiedName, Dataset<Row>> propertyDataframeMap;
//    private final ConcurrentMap<String, Dataset<Row>>            entitySetDataframes;

    @Inject
    public ConductorSparkImpl(
            String keyspace,
            SparkSession sparkSession,
            SparkContextJavaFunctions cassandraJavaContext,
            CassandraTableManager cassandraTableManager,
            EdmManager dataModelService,
            SparkAuthorizationManager authzManager,
            HazelcastInstance hazelcastInstance ) {
        this.sparkSession = sparkSession;
        this.cassandraJavaContext = cassandraJavaContext;
        this.authzManager = authzManager;
        this.keyspace = keyspace;
        this.cassandraTableManager = cassandraTableManager;
        this.dataModelService = dataModelService;
//        this.entityDataframeMap = Maps.newConcurrentMap();// hazelcastInstance.getMap(
//        // HazelcastNames.Maps.ENTITY_DATAFRAMES );
//        this.propertyDataframeMap = Maps.newConcurrentMap(); // hazelcastInstance.getMap(
//        // HazelcastNames.Maps.PROPERTY_DATAFRAMES );
//        this.entitySetDataframes = Maps.newConcurrentMap();// hazelcastInstance.getMap(
//        // HazelcastNames.Maps.ENTITY_SET_DATAFRAMES );
//        //prepareDataframe();
    }

//    private void prepareDataframe() {
//        dataModelService.getEntityTypes()
//                .forEach( entityType -> {
//                    String entityTableName = cassandraTableManager.getTablenameForEntityType( entityType );
//                    Dataset<Row> entityDf = sparkSession
//                            .read()
//                            .format( "org.apache.spark.sql.cassandra" )
//                            .option( "table", entityTableName.toLowerCase() )
//                            .option( "keyspace", keyspace )
//                            .load();
//                    entityDataframeMap.put( entityType.getFullQualifiedName(), entityDf );
//
//                     entityType.getProperties().forEach( fqn -> {
//                     String propertyTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( fqn );
//                     Dataset<Row> propertyDf = sparkSession
//                     .read()
//                     .format( "org.apache.spark.sql.cassandra" )
//                     .option( "table", propertyTableName )
//                     .option( "keyspace", keyspace )
//                     .load();
//                     propertyDataframeMap.put( fqn, propertyDf );
//                     } );
//                } );
//
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

//        Dataset<Row> entityDf = entityDataframeMap.get( entityFqn );

//        if ( entityDf == null ) {
//            entityDf = sparkSession
//                    .read()
//                    .format( "org.apache.spark.sql.cassandra" )
//                    .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
//                    .option( "keyspace", keyspace )
//                    .load();
//            entityDataframeMap.put( entityFqn, entityDf );
//        }
        Dataset<Row> entityDf = sparkSession
                            .read()
                            .format( "org.apache.spark.sql.cassandra" )
                            .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
                            .option( "keyspace", keyspace )
                            .load();
        List<String> columns = authorizedProperties.stream()
                .map( pt -> Queries.fqnToColumnName( pt.getFullQualifiedName() ) )
                // List<String> columns = authorizedProperties.stream().map( pt -> pt.getTypename() )
                .collect( Collectors.toList() );
        Preconditions.checkState( columns.size() > 0, "Must have access to at least one column." );
//        entityDf = entityDf.select( CommonColumns.ENTITYID.cql(), columns.toArray( new String[] {} ) );
//        entityDf.createOrReplaceTempView( "entityDf" );
        /*
         * entityDf = sparkSession .sql( "select " + CommonColumns.ENTITYID.cql() +
         * " from entityDf where array_contains( " + CommonColumns.ENTITY_SETS.cql() + ", '" + entitySetName + "')" );
         * List<Dataset<Row>> propertyDataFrames = authorizedProperties.stream() .map( property -> propertyDataframeMap
         * .get( property.getFullQualifiedName() ) .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() ) )
         * .collect( Collectors.toList() ); for ( Dataset<Row> rdf : propertyDataFrames ) { entityDf = entityDf.join(
         * rdf, scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
         * .toList(), LEFTOUTER ); }
         */
        String tableName = cacheToCassandra( entityDf, authorizedProperties );

        return new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() );
    }

    /**
     * Return QueryResult of UUID's ONLY of all entities matching a Look Up Entities Request.
     *
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's matching the lookup request
     */
    @Override
    public QueryResult getFilterEntities( LookupEntitiesRequest request ) {
        // Get set of JavaRDD of UUIDs matching the property value for each property type
        final Map<FullQualifiedName, QueryResult> dfs = Maps.newConcurrentMap();

        request.getEntityTypes().forEach( entityFqn -> {
            //Dataset<Row> entityDf = entityDataframeMap.get( entityFqn );

//            if ( entityDf == null ) {
//                entityDf = sparkSession
//                        .read()
//                        .format( "org.apache.spark.sql.cassandra" )
//                        .option( "table", cassandraTableManager.getTablenameForEntityType( entityFqn ) )
//                        .option( "keyspace", keyspace )
//                        .load();
//                entityDataframeMap.put( entityFqn, entityDf );
//            }
            Dataset<Row> entityDf = sparkSession
                                    .read()
                                    .format( "org.apache.spark.sql.cassandra" )
                                    .option( "table", cassandraTableManager.getTablenameForEntityType( entityFqn ) )
                                    .option( "keyspace", keyspace )
                                    .load();

            for ( Entry<FullQualifiedName, Object> e : request.getPropertyTypeToValueMap().entrySet() ) {
                entityDf = entityDf
                        .filter( entityDf.apply( Queries.fqnToColumnName( e.getKey() ) )
                                .equalTo( e.getValue() instanceof UUID ? e.getValue().toString() : e.getValue() ) );
            }
            String tableName = cacheToCassandra( entityDf,
                    cassandraTableManager.getEntityType( entityFqn ).getProperties().stream()
                            .map( cassandraTableManager::getPropertyType ).collect( Collectors.toList() ) );
            dfs.put( entityFqn,
                    new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() ) );
            entityDf.createOrReplaceTempView( "entityDf" );

        } );
        return dfs.values().iterator().next();

        // Set<JavaRDD<UUID>> resultsMatchingPropertyValues = request.getPropertyTypeToValueMap().entrySet()
        // .parallelStream()
        // .map( ptv -> getEntityIds( request.getUserId(),
        // cassandraTableManager.getTablenameForPropertyValuesOfType( ptv.getKey() ),
        // ptv.getValue() ) )
        // .collect( Collectors.toSet() );
        //
        // // Take intersection to get the JavaRDD of UUIDs matching all the property type values, but before filtering
        // // Entity Types
        // // TODO: repartitionbyCassandraReplica is not done, which means that intersection is potentially extremely
        // slow.
        // JavaRDD<UUID> resultsBeforeFilteringEntityTypes = resultsMatchingPropertyValues.stream()
        // .reduce( ( leftRDD, rightRDD ) -> leftRDD.intersection( rightRDD ) )
        // .get();
        //
        // // Get the RDD of UUIDs matching all the property type values, after filtering Entity Types
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

//        Dataset<Row> entityDf = entityDataframeMap.get( entityTypeFqn );
//
//        if ( entityDf == null ) {
//            entityDf = sparkSession
//                    .read()
//                    .format( "org.apache.spark.sql.cassandra" )
//                    .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
//                    .option( "keyspace", keyspace )
//                    .load();
//            entityDataframeMap.put( entityTypeFqn, entityDf );
//        }
        Dataset<Row> entityDf = sparkSession
                            .read()
                            .format( "org.apache.spark.sql.cassandra" )
                            .option( "table", cassandraTableManager.getTablenameForEntityType( entityType ) )
                            .option( "keyspace", keyspace )
                            .load();

        List<String> columns = authorizedProperties.stream()
                .map( pt -> Queries.fqnToColumnName( pt.getFullQualifiedName() ) )
                // List<String> columns = authorizedProperties.stream().map( pt -> pt.getTypename() )
                .collect( Collectors.toList() );
        Preconditions.checkState( columns.size() > 0, "Must have access to at least one column." );

        entityDf = entityDf.select( CommonColumns.ENTITYID.cql(), columns.toArray( new String[] {} ) );
        entityDf.createOrReplaceTempView( "entityDf" );

        /*
         * entityDf = entityDf.select( CommonColumns.ENTITYID.cql() ); List<Dataset<Row>> propertyDataFrames =
         * authorizedProperties.stream() .map( property -> propertyDataframeMap .get( property.getFullQualifiedName() )
         * .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() ) ) .collect( Collectors.toList() ); for (
         * Dataset<Row> rdf : propertyDataFrames ) { entityDf = entityDf.join( rdf,
         * scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) ) .toList(),
         * LEFTOUTER ); }
         */
        String tableName = cacheToCassandra( entityDf, authorizedProperties );

        return new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() );
    }

    private String cacheToCassandra( Dataset<Row> df, List<PropertyType> propertyTypes ) {
        List<String> columnNames = propertyTypes.stream()
                .map( pt -> "value_" + pt.getTypename() )
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

}
