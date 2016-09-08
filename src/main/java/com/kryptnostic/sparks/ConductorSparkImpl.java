package com.kryptnostic.sparks;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.datastax.driver.core.DataType;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaPairRDD;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.google.common.collect.Maps;
import com.kryptnostic.conductor.codecs.FullQualifiedNameTypeCodec;
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
import com.kryptnostic.mapstores.v2.Permission;

import scala.collection.IndexedSeq;
import scala.collection.JavaConversions;
import scala.collection.Seq;

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

    public QueryResult filterEntities( LookupEntitiesRequest request ) {
        // First we need to get object id RDDs from properties tables.

        Set<EntityType> entityTypes = request.getEntityTypes().stream()
                .map( dataModelService::getEntityType )
                .collect( Collectors.toSet() );
        // Tough part is looking up entities that support this type.
        Set<CassandraJavaPairRDD<UUID, String>> partitionKeys = request.getPropertyTypeToValueMap().entrySet()
                .parallelStream()
                .map( ptv -> getEntityIds( request.getUserId(),
                        cassandraTableManager.getTablenameForPropertyIndexOfType( ptv.getKey() ),
                        ptv.getValue() ) )
                .map( rdd -> CassandraJavaUtil.javaFunctions( rdd ).joinWithCassandraTable( keyspace,
                        Tables.ENTITY_ID_TO_TYPE.getTableName(),
                        CassandraJavaUtil.someColumns( CommonColumns.TYPENAME.cql() ),
                        CassandraJavaUtil.someColumns( CommonColumns.ENTITYID.cql() ),
                        CassandraJavaUtil.mapColumnTo( String.class ),
                        new RowWriterFactory<UUID>() {

                            @Override
                            public RowWriter<UUID> rowWriter( TableDef t, IndexedSeq<ColumnRef> colRefs ) {
                                return new RRU();
                            }
                        } ) )
                .collect( Collectors.toSet() );
        // keyspace,cassandraTableManager.getTablenameForEntityType( entityType ) , selectedColumns, joinColumns,
        // rowReaderFactory, rowWriterFactory ) ))
        // .collect( Collectors.toSet() );

        // String indexTable =
        // )
        // .joinWithCassandraTable( keyspace, indexTable, selectedColumns, joinColumns, rowReaderFactory,
        // rowWriterFactory );
        //partitionKeys.iterator().next().
        System.err.println( partitionKeys.iterator().next().collectAsMap().toString() );
        return null;
    }

    private JavaRDD<UUID> getEntityIds( UUID userId, String table, Object value ) {
        return cassandraJavaContext.cassandraTable( keyspace, table, CassandraJavaUtil.mapColumnTo( UUID.class ) )
                .select( CommonColumns.ENTITYID.cql() )
                .where( "value = ?", value )
                .distinct();
    }

    @Override
    public QueryResult loadAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );
        //        Map<FullQualifiedName, PropertyType> propertyTypenames = Maps.asMap( entityType.getProperties(),
        //                fqn -> dataModelService.getPropertyType( fqn ) );
        //        cassandraSqlContext.setKeyspace( keyspace );
        //        String query = StringUtils.remove(
        //                QueryBuilder.select( CommonColumns.ENTITYID.cql() )
        //                        .from( this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) ).toString(),
        //                ";" );
        //
        //        //Get DataFrame for entityid only
        //        DataFrame df = cassandraSqlContext.cassandraSql( query );
        //
        //        Stream<Entry<FullQualifiedName, PropertyType>> entryStream = propertyTypenames.entrySet().stream();
        //        CodecRegistry.DEFAULT_INSTANCE.register( new FullQualifiedNameTypeCodec() );
        //
        //        Map<FullQualifiedName, DataFrame> propertyDataframes = entryStream
        //                .collect( Collectors.toConcurrentMap(
        //                        e -> (FullQualifiedName) e.getKey(),
        //                        e -> {
        //                            logger.info( "Property Type: {}", e.getValue() );
        //                            String pTable = cassandraTableManager
        //                                    .getTablenameForPropertyValuesOfType( e.getValue() );
        //                            logger.info( "Ptable = {}", pTable );
        //                            String q = StringUtils.remove(
        //                                    QueryBuilder.select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() )
        //                                            .from( pTable )
        //                                            .toString(),
        //                                    ";" );
        //                            logger.info( "Property Type query = {}", q );
        //                            return cassandraSqlContext.cassandraSql( q );
        //                        } ) );

        // Hacking
        List<FullQualifiedName> propertyFqns = Lists.newLinkedList( entityType.getProperties() );

        List<PropertyType> propertyTypes = propertyFqns.stream().map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect(
                        Collectors.toList() );
        List<DataType> dataTypes = propertyTypes.stream().map( pt -> CassandraEdmMapping.getCassandraType( pt.getDatatype() ) )
                .collect(
                        Collectors.toList() );

        cassandraSqlContext.setKeyspace( keyspace );

        String query = StringUtils.remove(
                QueryBuilder.select( CommonColumns.ENTITYID.cql() )
                        .from( this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) ).toString(),
                ";" );
        DataFrame df = cassandraSqlContext.cassandraSql( query );
        List<DataFrame> dataFrames = propertyTypes.stream().map( pt -> {
            String pTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( pt );
            String q = StringUtils.remove( QueryBuilder.select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql()).from( pTableName ).toString(), ";" );
            return cassandraSqlContext.cassandraSql( q );
        } ).collect( Collectors.toList());

        // Hacking end
        for ( DataFrame rdf : dataFrames ) {
            df.show();
            rdf.show();
            df = df.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                            .toList(),
                    "leftouter" );
        }

        df.show();

        propertyFqns.add( 0, new FullQualifiedName( entityType.getNamespace(), "entityid" ) );
        dataTypes.add( 0, DataType.uuid() );
        String cacheTable = initializeTempTable( propertyFqns, dataTypes );
        
        /*
         * 1. columns order doesn't matter for Cassandra table, as long as DataType is matched
         * 2. column names order for DataFrame matters. we can give the columns any names we want.
         * 3. rowWriter will base on the names of its columns to lookup the cassandra column defs to get the DataType.
         * 4. .withColumnSelector(...) using the name we assigned to rowWriter.
         */

//        List<FullQualifiedName> columnNames = Lists.newArrayList();
//        columnNames.add( new FullQualifiedName( "test", "entityid" ) );
//        columnNames.add( new FullQualifiedName( "test", "salary" ) );
//        columnNames.add( new FullQualifiedName( "test", "employeename" ) );
//        columnNames.add( new FullQualifiedName( "test", "employeeid" ) );
//        columnNames.add( new FullQualifiedName( "test", "employeetitle" ) );
//        columnNames.add( new FullQualifiedName( "test", "employeedept" ) );

        CassandraJavaUtil.javaFunctions( df.toJavaRDD() ).writerBuilder( "cache",
                cacheTable,
                new RowWriterFactory<Row>() {
                    @Override public RowWriter<Row> rowWriter(
                            TableDef table, IndexedSeq<ColumnRef> selectedColumns ) {
                        return new CacheTableRowWriter( propertyFqns );
                    }
                } )
                //                .withConstantTTL( 2 * 60 * 60 * 1000 )
                .saveToCassandra();

        return null;
    }

    public String initializeTempTable( List<FullQualifiedName> propertyFqns, List<DataType> dataTypes ) {
        String tableName = "comeon8";
//        Set<PropertyType> propertyTypes = propertyFqns.stream().map( fqn -> dataModelService.getPropertyType( fqn ) )
//                .collect(
//                        Collectors.toSet() );
//        Map<FullQualifiedName, DataType> columnDefs = Maps.asMap( propertyFqns,
//                fqn -> CassandraEdmMapping.getCassandraType( dataModelService.getPropertyType( fqn ).getDatatype() ) );

        String query = new CacheTableBuilder( tableName ).columns( propertyFqns, dataTypes ).buildQuery();
        logger.info( query );

        CassandraConnector cassandraConnector = CassandraConnector.apply( spark.getConf() );
        try ( Session session = cassandraConnector.openSession() ) {
            session.execute( "DROP TABLE IF EXISTS cache.comeon6" );
            session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS cache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}" );
            session.execute( query );
//            session.execute(
//                    " CREATE TABLE cache.comeon6 ( employeedept text, entityid uuid, employeeid uuid,  employeetitle text, employeename text, salary bigint, PRIMARY KEY ( entityid ) )" );
        }
        return tableName;
    }

}
