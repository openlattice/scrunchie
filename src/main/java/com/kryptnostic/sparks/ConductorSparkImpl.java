package com.kryptnostic.sparks;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.ImmutableList;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.LoadEntitiesRequest;
import com.kryptnostic.conductor.rpc.UUIDs.ACLs;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class ConductorSparkImpl implements ConductorSparkApi {
    private static final Logger             logger = LoggerFactory.getLogger( ConductorSparkImpl.class );
    private final JavaSparkContext          spark;
    private final CassandraSQLContext       cassandraContext;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final SparkAuthorizationManager authzManager;

    @Inject
    public ConductorSparkImpl( JavaSparkContext spark, SparkAuthorizationManager authzManager ) {
        this.spark = spark;
        this.cassandraContext = new CassandraSQLContext( spark.sc() );
        this.cassandraJavaContext = javaFunctions( spark );
        this.authzManager = authzManager;
    }

    @Override
    public List<UUID> lookupEntities( String keyspace, LoadEntitiesRequest entityKey ) {
        return entityKey.getPropertyTableToValueMap().entrySet().stream()
                .map( e -> cassandraJavaContext.cassandraTable( keyspace,
                        e.getKey(),
                        CassandraJavaUtil.mapColumnTo( UUID.class ) )
                        .select( CommonColumns.OBJECTID.cql() ).where( "value = ? and aclId IN ?",
                                e.getValue(),
                                ImmutableList.of( ACLs.EVERYONE_ACL ) )
                        .distinct() )
                .reduce( ( lhs, rhs ) -> lhs.intersection( rhs ) ).get().collect();

//        return entityKey.getPropertyTableToValueMap().entrySet().stream()
//                .map( e -> cassandraContext
//                        .cassandraSql( QueryBuilder
//                                .select( CommonColumns.OBJECTID.cql() ).distinct()
//                                .from( keyspace, e.getKey() )
//                                .where( QueryBuilder.eq( CommonColumns.VALUE.cql(), e.getValue() ) )
//                                .and( QueryBuilder.in( CommonColumns.ACLID.cql(),
//                                        authzManager.getAuthorizedAcls( entityKey.getUserId(),
//                                                Permission.READ ) ) )
//                                .getQueryString() )
//                        .toJavaRDD().map( row -> UUID.fromString( row.getAs( CommonColumns.OBJECTID.cql() ) ) ) )
//                .reduce( ( lhs, rhs ) -> lhs.intersection( rhs ) ).get().collect();
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
