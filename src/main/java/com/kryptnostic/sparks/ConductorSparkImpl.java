package com.kryptnostic.sparks;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.apache.spark.sql.catalyst.plans.JoinType;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.kryptnostic.conductor.codecs.FullQualifiedNameTypeCodec;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.EntityType;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.mapstores.v2.Permission;

import breeze.linalg.ptp;
import jersey.repackaged.com.google.common.collect.Lists;

public class ConductorSparkImpl implements ConductorSparkApi {
    private static final Logger             logger = LoggerFactory.getLogger( ConductorSparkImpl.class );
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
        return entityKey.getPropertyTableToValueMap().entrySet().stream()
                .map( e -> cassandraJavaContext.cassandraTable( keyspace,
                        e.getKey(),
                        CassandraJavaUtil.mapColumnTo( UUID.class ) )
                        .select( CommonColumns.OBJECTID.cql() ).where( "value = ? and aclId IN ?",
                                e.getValue(),
                                authzManager.getAuthorizedAcls( userId, Permission.READ ) )
                        .distinct() )
                .reduce( ( lhs, rhs ) -> lhs.intersection( rhs ) ).get().collect();
    }

    @Override
    public QueryResult loadEntitySet( EntitySet setType ) {
        cassandraSqlContext.setKeyspace( keyspace );
        DataFrame df = cassandraSqlContext.cassandraSql( "select * from entity_nbo9mf6nml3p49zq21funofw" )
                .where( new Column( "clock" ).geq( "2016-09-03 00:51:42" ) );
        JavaRDD<String> rdd = new JavaRDD<String>( df.toJSON(), scala.reflect.ClassTag$.MODULE$.apply( String.class ) );// CassandraJavaUtil.javaFunctions(
        // df.toJSON() )
        rdd.foreach( s -> System.err.println( s ) );
        // JavaRDD<CassandraRow> rdd =cassandraJavaContext.cassandraTable( keyspace, "entitySetMembership" ).select(
        // "entityIds" )
        // .where( "name = ? AND type = ?", setType.getName(), setType.getType().getFullQualifiedNameAsString() );
        // cassandraSqlContext.cass
        // cassandraSqlContext.createDataFrame(rdd,new StructType() );
        return null;
    }

    private List<PropertyType> loadPropertiesOfType( String namespace, String entityName ) {
        List<PropertyType> targetPropertyTypes = dataModelService.getEntityType( namespace, entityName ).getProperties()
                .stream()
                .map( e -> dataModelService.getPropertyType( e ) ).collect( Collectors.toList() );

        return targetPropertyTypes;
    }

    public QueryResult loadEntitySet( String namespace, String entityName ) {

        return null;
    }

    private String initializeTempTable( String entityName, List<PropertyType> propertyTypes ) {
        String tableName = "";

        // TODO: construct the CQL for create table
        StringBuilder sb = new StringBuilder( "CREATE TABLE cache." );
        sb.append( tableName );
        sb.append( " (" );
        for ( PropertyType pt : propertyTypes ) {
            sb.append( pt.getFullQualifiedName() );
            sb.append( " " );
            sb.append( pt.getDatatype().toString() );
            sb.append( ", " );
        }
        // TODO: formatting CQL and add PRIMARY KEY

        CassandraConnector cassandraConnector = CassandraConnector.apply( spark.getConf() );
        try ( Session session = cassandraConnector.openSession() ) {
            session.execute( "DROP KEYSPACE IF EXISTS cache" );
            session.execute(
                    "CREATE KEYSPACE cache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1" );
            session.execute( sb.toString() );
        }
        return null;
    }

    @Override
    public QueryResult loadAllEntitiesOfType( FullQualifiedName entityTypeFqn ) {
        EntityType entityType = dataModelService.getEntityType( entityTypeFqn.getNamespace(), entityTypeFqn.getName() );
        Map<FullQualifiedName, PropertyType> propertyTypenames = Maps.asMap( entityType.getProperties(),
                fqn -> dataModelService.getPropertyType( fqn ) );
        cassandraSqlContext.setKeyspace( keyspace );
        String query = StringUtils.remove(
                QueryBuilder.select( CommonColumns.OBJECTID.cql() )
                        .from( this.cassandraTableManager.getTablenameForEntityType( entityTypeFqn ) ).toString(),
                ";" );
        logger.error( "Query = {}", query );
        DataFrame df = cassandraSqlContext.cassandraSql( query );
        Stream<Entry<FullQualifiedName, PropertyType>> entryStream = propertyTypenames.entrySet().stream();
        Map<FullQualifiedName, DataFrame> propertyDataframes = entryStream
                .collect( Collectors.toConcurrentMap(
                        e -> (FullQualifiedName) e.getKey(),
                        e -> {
                            CodecRegistry.DEFAULT_INSTANCE.register( new FullQualifiedNameTypeCodec() );
                            logger.info( "Property Type: {}", e.getValue() );
                            String pTable = cassandraTableManager
                                    .getTablenameForPropertyType( e.getValue() );
                            logger.info( "Ptable = {}", pTable );
                            String q = StringUtils.remove(
                                    QueryBuilder.select( CommonColumns.OBJECTID.cql(), CommonColumns.VALUE.cql() )
                                            .from( pTable )
                                            .toString(),
                                    ";" );
                            logger.info( "Property Type query = {}", q );
                            return cassandraSqlContext.cassandraSql( q );
                        } ) );
        for ( DataFrame rdf : propertyDataframes.values() ) {
            df.show();
            rdf.show();
            df = df.join( rdf,
                    scala.collection.JavaConversions.asScalaBuffer( Arrays.asList( CommonColumns.OBJECTID.cql() ) )
                            .toList(),
                    "leftouter" );
        }

        df.show();

        JavaRDD<String> rdd = new JavaRDD<String>(
                df.toJSON(),
                scala.reflect.ClassTag$.MODULE$.apply( String.class ) );
        rdd.foreach( s -> System.err.println( s ) );

        // rdd = new JavaRDD<String>( df.toJSON(), scala.reflect.ClassTag$.MODULE$.apply( String.class ) );
        // rdd.foreach( s -> System.err.println( s ) );
        return null;
    }

    //
    // @Override
    // public List<Employee> processEmployees() {
    // JavaRDD<Employee> t = spark.textFile( "kryptnostic-conductor/src/main/resources/employees.csv" )
    // .map( e -> Employee.EmployeeCsvReader.getEmployee( e ) );
    // SQLContext context = new SQLContext( spark );
    // logger.info( "Total # of employees: {}", t.count() );
    // DataFrame df = context.createDataFrame( t, Employee.class );
    // df.registerTempTable( "employees" );
    // DataFrame emps = context.sql( "SELECT * from employees WHERE salary > 81500" );
    // List<String> highlyPaidEmps = emps.javaRDD().map( e -> String.format( "%s,%s,%s,%d",
    // e.getAs( "name" ),
    // e.getAs( "dept" ),
    // e.getAs( "title" ),
    // e.getAs( "salary" ) ) ).collect();
    // highlyPaidEmps.forEach( e -> logger.info( e ) );
    //
    // return Lists.newArrayList( emps.javaRDD().map( e -> new Employee(
    // e.getAs( "name" ),
    // e.getAs( "dept" ),
    // e.getAs( "title" ),
    // (int) e.getAs( "salary" ) ) ).collect() );
    // }

}
