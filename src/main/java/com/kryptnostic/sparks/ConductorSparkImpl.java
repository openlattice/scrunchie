package com.kryptnostic.sparks;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.ArrayContains;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.IndexedSeq;

import javax.inject.Inject;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class ConductorSparkImpl implements ConductorSparkApi, Serializable {
    private static final long         serialVersionUID = 825467486008335571L;
    private static final Logger       logger           = LoggerFactory.getLogger( ConductorSparkImpl.class );
    private static final String       LEFTOUTER        = "leftouter";
    private final        SecureRandom random           = new SecureRandom();
    private static final String       CACHE_KEYSPACE   = "cache";

    private final SparkSession              sparkSession;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final SparkAuthorizationManager authzManager;
    private final String                    keyspace;
    private final CassandraTableManager     cassandraTableManager;
    private final EdmManager                dataModelService;

    private final ConcurrentMap<FullQualifiedName, Dataset<Row>> entityDataframeMap;
    private final ConcurrentMap<FullQualifiedName, Dataset<Row>> propertyDataframeMap;

    @Inject
    public ConductorSparkImpl(
            String keyspace,
            SparkSession sparkSession,
            SparkContextJavaFunctions cassandraJavaContext,
            CassandraTableManager cassandraTableManager,
            EdmManager dataModelService,
            SparkAuthorizationManager authzManager ) {
        this.sparkSession = sparkSession;
        this.cassandraJavaContext = cassandraJavaContext;
        this.authzManager = authzManager;
        this.keyspace = keyspace;
        this.cassandraTableManager = cassandraTableManager;
        this.dataModelService = dataModelService;
        this.entityDataframeMap = Maps.newConcurrentMap();
        this.propertyDataframeMap = Maps.newConcurrentMap();
        prepareDataframe();
    }

    private void prepareDataframe() {
        Set<EntityType> entityTypes = Sets.newHashSet();
        dataModelService.getSchemas().forEach( schema ->
                entityTypes.addAll( schema.getEntityTypes() ) );
        entityTypes.forEach( entityType -> {
            String entityTableName = cassandraTableManager.getTablenameForEntityType( entityType );
            Dataset<Row> entityDf = sparkSession
                    .read()
                    .format( "org.apache.spark.sql.cassandra" )
                    .option( "table", entityTableName )
                    .option( "keyspace", keyspace )
                    .load();
            entityDataframeMap.put( entityType.getFullQualifiedName(), entityDf );

            entityType.getProperties().forEach( fqn -> {
                String propertyTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( fqn );
                Dataset<Row> propertyDf = sparkSession
                        .read()
                        .format( "org.apache.spark.sql.cassandra" )
                        .option( "table", propertyTableName )
                        .option( "keyspace", keyspace )
                        .load();
                propertyDataframeMap.put( fqn, propertyDf );
            } );
        } );

    }

    @Override
    public List<UUID> lookupEntities( LookupEntitiesRequest entityKey ) {
        return entityKey.getPropertyTypeToValueMap().entrySet().stream()
                .map( e -> cassandraJavaContext.cassandraTable( keyspace,
                        cassandraTableManager.getTablenameForPropertyIndexOfType( e.getKey() ),
                        CassandraJavaUtil.mapColumnTo( UUID.class ) )
                        .select( CommonColumns.ENTITYID.cql() ).where( "value = ?",
                                e.getValue() )
                        .distinct() )
                .reduce( ( lhs, rhs ) -> lhs.intersection( rhs ) ).get().collect();
    }
    
    @Override
    public List<UUID> loadEntitySet( FullQualifiedName fqn ) {
        return cassandraJavaContext.cassandraTable(
                keyspace,
                cassandraTableManager.getTablenameForEntityType( fqn ),
                CassandraJavaUtil.mapColumnTo( UUID.class ) )
                .select( CommonColumns.ENTITYID.cql() )
                .distinct()
                .collect();
    }

    @Override public QueryResult getAllEntitiesOfEntitySet( FullQualifiedName entityFqn, String entitySetName ) {
        EntityType entityType = dataModelService.getEntityType( entityFqn );
        List<FullQualifiedName> propertyFqns = Lists.newLinkedList(entityType.getProperties());
        List<PropertyType> propertyTypes = propertyFqns.stream()
                .map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect( Collectors.toList() );

        Dataset<Row> entityDf = entityDataframeMap.get( entityFqn );
        entityDf.createOrReplaceTempView( "entityDf" );
        entityDf = sparkSession.sql( "select entityid from entityDf where array_contains( entitysets, '" + entitySetName + "')" );

        List<Dataset<Row>> propertyDataFrames = propertyFqns.stream()
                .map( fqn -> propertyDataframeMap
                        .get( fqn )
                        .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() )
                ).collect( Collectors.toList() );

        for ( Dataset<Row> rdf : propertyDataFrames ) {
            entityDf = entityDf.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                            .toList(),
                    LEFTOUTER );
        }
        String tableName = cacheToCassandra( entityDf, propertyTypes );

        return new QueryResult( CACHE_KEYSPACE, tableName, UUID.randomUUID(), UUID.randomUUID().toString() );
    }
    
    /**
     * Return QueryResult of UUID's ONLY of all entities matching a Look Up Entities Request.
     * @param request A LookupEntitiesRequest object
     * @return QueryResult of UUID's matching the lookup request
     */
    public QueryResult filterEntities( LookupEntitiesRequest request ) {
    	//Get set of JavaRDD of UUIDs matching the property value for each property type
        Set<JavaRDD<UUID>> resultsMatchingPropertyValues = request.getPropertyTypeToValueMap().entrySet()
                .parallelStream()
                .map( ptv -> getEntityIds( request.getUserId(),
                        cassandraTableManager.getTablenameForPropertyValuesOfType( ptv.getKey() ),
                        ptv.getValue() ) )
                .collect( Collectors.toSet() );
        //Take intersection to get the JavaRDD of UUIDs matching all the property type values, but before filtering Entity Types
    	//TODO: repartitionbyCassandraReplica is not done, which means that intersection is potentially extremely slow.
            JavaRDD<UUID> resultsBeforeFilteringEntityTypes = resultsMatchingPropertyValues.stream()
                .reduce( (leftRDD, rightRDD) -> leftRDD.intersection( rightRDD ) )
                .get();

        //Get the RDD of UUIDs matching all the property type values, after filtering Entity Types
        //TODO: once Hristo's entity type to entity id table is done, maybe faster to use that rather than do multiple joinWithCassandraTable
        //Looks like JavaSparkContext is not injected anymore.    
            JavaRDD<UUID> resultsAfterFilteringEntityTypes = ( new JavaSparkContext(sparkSession.sparkContext()) ).emptyRDD();
        
            if( !resultsBeforeFilteringEntityTypes.isEmpty() ){
                resultsAfterFilteringEntityTypes = request.getEntityTypes()
                	.stream()
                    .map( typeFQN -> cassandraTableManager.getTablenameForEntityType( typeFQN ) )
                    .map( typeTablename -> CassandraJavaUtil.javaFunctions( resultsBeforeFilteringEntityTypes )
                            .joinWithCassandraTable( keyspace, 
                                typeTablename, 
                                CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ), 
                                CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ),
                                CassandraJavaUtil.mapColumnTo( UUID.class ), 
                                //RowWriter not really necessary - should not be invoked during lazy evaluation.
                                new RowWriterFactory<UUID>() {
    
                                    @Override
                                    public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
                                        return new RowWriterForUUID();
                                    }
                                }
                            ).keys()
                        )
                    .reduce( (leftRDD, rightRDD) -> leftRDD.union( rightRDD ) )
                    .get();
            }
        
        // Write to QueryResult
        // Build Temp Table, using Yao's initializeTempTable function
        // Initialize Temp Table
            String cacheTable = initializeTempTable(
                    Collections.singletonList( CommonColumns.ENTITYID.cql() ),
                    Collections.singletonList( DataType.uuid() ) 
                    );
        // Save RDD of entityID's to Cassandra.    
            CassandraJavaUtil.javaFunctions( resultsAfterFilteringEntityTypes )
                    .writerBuilder( CACHE_KEYSPACE,
                            cacheTable,
                            //toModify
                            new RowWriterFactory<UUID>() {
                                @Override
                                public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
                                    return new RowWriterForUUID();
                                }
                            })
                    .saveToCassandra();
        
        // Return Query Result pointing to the temp table.
        return new QueryResult( CACHE_KEYSPACE, cacheTable, UUID.randomUUID(), UUID.randomUUID().toString());
    }


    private JavaRDD<UUID> getEntityIds( UUID userId, String table, Object value ) {
        return cassandraJavaContext.cassandraTable( keyspace, table, CassandraJavaUtil.mapColumnTo( UUID.class ) )
                .select( CommonColumns.ENTITYID.cql() )
                .where( "value = ?", value )
                .distinct();
    }

    @Override
    public QueryResult loadAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn );
        List<FullQualifiedName> propertyFqns = Lists.newLinkedList( entityType.getProperties() );
        List<PropertyType> propertyTypes = propertyFqns.stream()
                .map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect( Collectors.toList() );

        Dataset<Row> entityDf = entityDataframeMap.get( entityTypeFqn );
        entityDf = entityDf.select( CommonColumns.ENTITYID.cql() );

        List<Dataset<Row>> propertyDataFrames = propertyFqns.stream()
                .map( fqn -> propertyDataframeMap
                        .get( fqn )
                        .select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() )
                ).collect( Collectors.toList() );

        for ( Dataset<Row> rdf : propertyDataFrames ) {
            entityDf = entityDf.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                            .toList(),
                    LEFTOUTER );
        }
        String tableName = cacheToCassandra( entityDf, propertyTypes );

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
                        new RowWriterFactory<Row>() {
                            @Override
                            public RowWriter<Row> rowWriter(
                                    TableDef table,
                                    IndexedSeq<ColumnRef> selectedColumns ) {
                                return new CacheTableRowWriter( columnNames );
                            }
                        } )
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
