package com.kryptnostic.sparks;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.datastax.driver.core.DataType;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.PairRDDJavaFunctions;
import com.datastax.spark.connector.japi.RDDJavaFunctions;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;

import scala.collection.IndexedSeq;

public class ConductorSparkImpl implements ConductorSparkApi, Serializable {
    private static final long   serialVersionUID = 825467486008335571L;
    private static final Logger logger           = LoggerFactory.getLogger( ConductorSparkImpl.class );
    private static final String CACHE_KEYSPACE   = "cache";

    private final JavaSparkContext          spark;
    private final CassandraSQLContext       cassandraSqlContext;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final SparkAuthorizationManager authzManager;
    private final String                    keyspace;
    private final CassandraTableManager     cassandraTableManager;
    private final EdmManager                dataModelService;

    @Inject
    public ConductorSparkImpl(
            String keyspace,
            JavaSparkContext spark,
            CassandraSQLContext cassandraSqlContext,
            SparkContextJavaFunctions cassandraJavaContext,
            CassandraTableManager cassandraTableManager,
            EdmManager dataModelService,
            SparkAuthorizationManager authzManager ) {
        this.spark = spark;
        this.cassandraSqlContext = cassandraSqlContext;
        this.cassandraJavaContext = cassandraJavaContext;
        this.authzManager = authzManager;
        this.keyspace = keyspace;
        this.cassandraTableManager = cassandraTableManager;
        this.dataModelService = dataModelService;
    }

    @Override
    public List<UUID> lookupEntities( LookupEntitiesRequest entityKey ) {
        UUID userId = entityKey.getUserId();
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
    public QueryResult loadEntitySet( EntitySet setType ) {
        cassandraSqlContext.setKeyspace( keyspace );
        DataFrame df = cassandraSqlContext.cassandraSql( "select * from entity_nbo9mf6nml3p49zq21funofw" )
                .where( new Column( "clock" ).geq( "2016-09-03 00:51:42" ) );
        JavaRDD<String> rdd = new JavaRDD<String>( df.toJSON(), scala.reflect.ClassTag$.MODULE$.apply( String.class ) );
        // JavaRDD<CassandraRow> rdd =cassandraJavaContext.cassandraTable( keyspace, "entitySetMembership" ).select(
        // "entityIds" )
        // .where( "name = ? AND type = ?", setType.getName(), setType.getType().getFullQualifiedNameAsString() );
        // cassandraSqlContext.cass
        // cassandraSqlContext.createDataFrame(rdd,new StructType() );
        return null;
    }

    /**
     * 
     * @param request a LookupEntitiesRequest from front end
     * @return QueryResult pointing to a Cassandra table of entityIDs of results
     */

    public QueryResult filterEntities( LookupEntitiesRequest request ) {
        Set<JavaRDD<UUID>> resultsMatchingPropertyValues = request.getPropertyTypeToValueMap().entrySet()
                .parallelStream()
                .map( ptv -> getEntityIds( request.getUserId(),
                        cassandraTableManager.getTablenameForPropertyValuesOfType( ptv.getKey() ),
                        ptv.getValue() ) )
                .collect( Collectors.toSet() );
        //Get the RDD of UUIDs matching all the property type values, but before filtering Entity Types
            JavaRDD<UUID> resultsBeforeFilteringEntityTypes = resultsMatchingPropertyValues.stream()
                .reduce( (leftRDD, rightRDD) -> leftRDD.intersection( rightRDD ) )
                .get();

        //Get the RDD of UUIDs matching all the property type values, after filtering Entity Types
        //TO CHANGE: once Hristo's entity type to entity id table is done, maybe faster to use that rather than do multiple joinWithCassandraTable
            JavaRDD<UUID> resultsAfterFilteringEntityTypes = spark.emptyRDD();
        
            if( !resultsBeforeFilteringEntityTypes.isEmpty() ){
                resultsAfterFilteringEntityTypes = request.getEntityTypes().stream()
                    .map( typeFQN -> cassandraTableManager.getTablenameForEntityType( typeFQN ) )
                    .map( typeTablename -> CassandraJavaUtil.javaFunctions( resultsBeforeFilteringEntityTypes )
                            .joinWithCassandraTable( keyspace, 
                                typeTablename, 
                                CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ), 
                                CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ),
                                //unnecessary rowReader - it's fine to write anything here since we will use keys() to drop the value column very quickly
                                CassandraJavaUtil.mapColumnTo( UUID.class ), 
                                new RowWriterFactory<UUID>() {
    
                                    @Override
                                    public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
                                        return new RRU();
                                    }
                                }
                            ).keys()
                        )
                    .reduce( (leftRDD, rightRDD) -> leftRDD.union( rightRDD ) )
                    .get();
            }
        
        // Write to QueryResult
        // Build Temp Table, using Yao's initializeTempTable function; randomUUID to generate unique temp table name for now
            String cacheTable = initializeTempTable(
                    getValidTableName( UUID.randomUUID() ),
                    Collections.singletonList( CommonColumns.ENTITYID.cql() ),
                    Collections.singletonList( DataType.uuid() ) 
                    );
        // Save RDD of entityID's to Cassandra.    
            CassandraJavaUtil.javaFunctions( resultsAfterFilteringEntityTypes )
                    .writerBuilder( "cache",
                            cacheTable,
                            //toModify
                            new RowWriterFactory<UUID>() {
                                @Override
                                public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
                                    return new RRU();
                                }
                            })
                    .saveToCassandra();
        
        // Return Query Result pointing to the temp table.
        // The param entitySet should be gone, after Yao's pull request got merged
        return new QueryResult( "cache",
                    cacheTable,
                    null, //query id
                    null, //session id
                    null, //entity Set
                    Optional.absent() );
    }

    private JavaRDD<UUID> getEntityIds( UUID userId, String table, Object value ) {
        return cassandraJavaContext.cassandraTable( keyspace, table, CassandraJavaUtil.mapColumnTo( UUID.class ) )
                .select( CommonColumns.ENTITYID.cql() )
                .where( "value = ?", value )
//TO DO: Repartitioning is extremely important here to make sure that entityID is used as the partition key
//                .repartitionbyCassandraReplica()
                .distinct();
    }

    @Override
    public QueryResult loadAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );

        List<FullQualifiedName> propertyFqns = Lists.newLinkedList( entityType.getProperties() );

        List<PropertyType> propertyTypes = propertyFqns.stream().map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect(
                        Collectors.toList() );
        List<DataType> propertyDataTypes = propertyTypes.stream()
                .map( pt -> CassandraEdmMapping.getCassandraType( pt.getDatatype() ) )
                .collect(
                        Collectors.toList() );

        cassandraSqlContext.setKeyspace( keyspace );

        String query = StringUtils.remove(
                QueryBuilder.select( CommonColumns.ENTITYID.cql() )
                        .from( this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) ).toString(),
                ";" );
        DataFrame df = cassandraSqlContext.cassandraSql( query );

        List<DataFrame> propertyDataFrames = propertyTypes.stream().map( pt -> {
            String pTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( pt );
            String q = StringUtils
                    .remove( QueryBuilder.select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() )
                            .from( pTableName ).toString(), ";" );
            return cassandraSqlContext.cassandraSql( q );
        } ).collect( Collectors.toList() );

        for ( DataFrame rdf : propertyDataFrames ) {
            df = df.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                            .toList(),
                    "leftouter" );
        }

        List<String> columnNames = propertyFqns.stream().map( fqn -> fqn.getName() ).collect( Collectors.toList() );
        columnNames.add( 0, "entityid" );
        propertyDataTypes.add( 0, DataType.uuid() );
        String cacheTable = initializeTempTable(
                getValidTableName( null ),
                columnNames,
                propertyDataTypes );
        
        /*
         * 1. columns order doesn't matter for Cassandra table, as long as DataType is matched
         * 2. column names order for DataFrame matters. we can give the columns any names we want.
         * 3. rowWriter will base on the names of its columns to lookup the cassandra column defs to get the DataType.
         * 4. .withColumnSelector(...) using the name we assigned to rowWriter.
         */
        CassandraJavaUtil.javaFunctions( df.toJavaRDD() )
                .writerBuilder( "cache",
                        cacheTable,
                        new RowWriterFactory<Row>() {
                            @Override public RowWriter<Row> rowWriter(
                                    TableDef table, IndexedSeq<ColumnRef> selectedColumns ) {
                                return new CacheTableRowWriter( columnNames );
                            }
                        } )
                //                .withConstantTTL( 2 * 60 * 60 * 1000 )
                .saveToCassandra();

        return new QueryResult( "cache",
                cacheTable,
                null,
                null,
                dataModelService.getEntitySet( entityTypeFqn, entityType.getTypename() ),
                Optional.absent() );
    }

    public String initializeTempTable( String tableName, List<String> columnNames, List<DataType> dataTypes ) {
        String query = new CacheTableBuilder( tableName ).columns( columnNames, dataTypes ).buildQuery();
        CassandraConnector cassandraConnector = CassandraConnector.apply( spark.getConf() );
        try ( Session session = cassandraConnector.openSession() ) {
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS cache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" );
            session.execute( query );
        }
        return tableName;
    }

    // TODO: move to Util and redesign
    // modified by Ho Chung to create hopefully distinct table name for now
    private String getValidTableName( UUID queryId ) {
        return "exclusive_pigg" + (queryId.hashCode() & 0xfffffff);
    }

}
