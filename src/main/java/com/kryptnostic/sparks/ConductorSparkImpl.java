package com.kryptnostic.sparks;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.kryptnostic.conductor.rpc.QueryResult;
import com.kryptnostic.conductor.rpc.odata.EntitySet;
import com.kryptnostic.conductor.rpc.odata.PropertyType;
import com.kryptnostic.datastore.cassandra.CassandraTableBuilder;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LookupEntitiesRequest;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.mapstores.v2.Permission;

public class ConductorSparkImpl implements ConductorSparkApi {
    private static final Logger logger = LoggerFactory.getLogger( ConductorSparkImpl.class );
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
        return null;
    }

    private List<PropertyType> loadPropertiesOfType(String namespace, String entityName) {
        List<PropertyType> targetPropertyTypes = dataModelService.getEntityType( namespace, entityName ).getProperties()
                .stream()
                .map( e -> dataModelService.getPropertyType( e ) ).collect( Collectors.toList());

        return targetPropertyTypes;
    }

    public QueryResult loadEntitySet( String namespace, String entityName ) {




        return null;
    }

    private String initializeTempTable( String entityName, List<PropertyType> propertyTypes ) {
        String tableName = "";

        //TODO: construct the CQL for create table
        StringBuilder sb = new StringBuilder( "CREATE TABLE cache." );
        sb.append( tableName );
        sb.append( " (" );
        for( PropertyType pt: propertyTypes){
            sb.append( pt.getFullQualifiedName() );
            sb.append( " " );
            sb.append( pt.getDatatype().toString() );
            sb.append( ", " );
        }
        //TODO: formatting CQL and add PRIMARY KEY

        CassandraConnector cassandraConnector = CassandraConnector.apply( spark.getConf() );
        try( Session session = cassandraConnector.openSession()){
            session.execute( "DROP KEYSPACE IF EXISTS cache" );
            session.execute( "CREATE KEYSPACE cache WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1" );
            session.execute( sb.toString() );
        }
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
