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
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.IndexedSeq;

import javax.inject.Inject;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
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
    public QueryResult loadEntitySet( EntitySet setType ) {
        Dataset<Row> df = sparkSession.sql( "select * from " + keyspace + ".entity_nbo9mf6nml3p49zq21funofw" )
                .where( new Column( "clock" ).geq( "2016-09-03 00:51:42" ) );
        JavaRDD<String> rdd = df.toJSON().toJavaRDD();

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
        // keyspace,cassandraTableManager.getTablenameForEntityType( entityType ) , selectedColumns,
        // joinColumns,
        // rowReaderFactory, rowWriterFactory ) ))
        // .collect( Collectors.toSet() );

        // String indexTable =
        // )
        // .joinWithCassandraTable( keyspace, indexTable, selectedColumns, joinColumns, rowReaderFactory,
        // rowWriterFactory );
        // partitionKeys.iterator().next().

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

        return new QueryResult( CACHE_KEYSPACE, tableName, null, null );
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
