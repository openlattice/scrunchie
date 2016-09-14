package com.kryptnostic.sparks;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.datastax.driver.core.DataType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.kryptnostic.datastore.cassandra.CassandraEdmMapping;
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final long         serialVersionUID = 825467486008335571L;
    private static final Logger       logger           = LoggerFactory.getLogger( ConductorSparkImpl.class );
    public static final  String       LEFTOUTER        = "leftouter";
    private final        SecureRandom random           = new SecureRandom();
    private static final String       CACHE_KEYSPACE   = "cache";

    private final JavaSparkContext          spark;
    private final SQLContext                cassandraSqlContext;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final SparkAuthorizationManager authzManager;
    private final String                    keyspace;
    private final CassandraTableManager     cassandraTableManager;
    private final EdmManager                dataModelService;

    @Inject
    public ConductorSparkImpl(
            String keyspace,
            JavaSparkContext spark,
            SQLContext cassandraSqlContext,
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
        Dataset<Row> df = cassandraSqlContext.sql( "select * from " + keyspace + ".entity_nbo9mf6nml3p49zq21funofw" )
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
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );

        List<FullQualifiedName> propertyFqns = Lists.newLinkedList( entityType.getProperties() );

        List<PropertyType> propertyTypes = propertyFqns.stream().map( fqn -> dataModelService.getPropertyType( fqn ) )
                .collect(
                        Collectors.toList() );

        String query = StringUtils.remove(
                QueryBuilder.select( CommonColumns.ENTITYID.cql() )
                        .from( keyspace, this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) )
                        .toString(),
                ";" );

        //Dataset<Row> df = cassandraSqlContext.sql( query );
        Dataset<Row> df = cassandraSqlContext
                .read()
                .format( "org.apache.spark.sql.cassandra" )
                .option( "table", this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) )
                .option( "keyspace", keyspace )
                .load();
        df.registerTempTable( "entityIds" );
        df =  df.select( CommonColumns.ENTITYID.cql() );

        List<Dataset<Row>> propertyDataFrames = propertyTypes.stream().map( pt -> {
            String pTableName = cassandraTableManager.getTablenameForPropertyValuesOfType( pt );
            String q = StringUtils
                    .remove( QueryBuilder.select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() )
                            .from( pTableName ).toString(), ";" );
            //return cassandraSqlContext.sql( q );
            return cassandraSqlContext.read()
                    .format( "org.apache.spark.sql.cassandra" )
                    .option( "table", pTableName )
                    .option( "keyspace", keyspace )
                    .load().select( CommonColumns.ENTITYID.cql(), CommonColumns.VALUE.cql() );
        } ).collect( Collectors.toList() );

        for ( Dataset<Row> rdf : propertyDataFrames ) {
            df = df.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                            .toList(),
                    LEFTOUTER );
        }

        String tableName = cacheToCassandra( df, propertyTypes );

        return new QueryResult(
                CACHE_KEYSPACE,
                tableName,
                null,
                null );
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
        CassandraConnector cassandraConnector = CassandraConnector.apply( spark.getConf() );
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
