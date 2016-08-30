package com.kryptnostic.sparks;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.cassandra.CassandraSQLContext;
import org.junit.BeforeClass;

import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.edm.BootstrapDatastoreWithCassandra;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;

public class BaseKindlingSparkTest extends BootstrapDatastoreWithCassandra {
    // Need to start Cassandra/Spark/Datastore
    protected static SparkConf                 conf;
    protected static SparkContext              spark;
    protected static JavaSparkContext          javaContext;
    protected static CassandraSQLContext       cassandraContext;
    protected static SparkContextJavaFunctions cassandraJavaContext;
    protected static SparkAuthorizationManager authzManager;
    protected static ConductorSparkImpl        csi;

    @BeforeClass
    public static void initSpark() {
        CassandraConfiguration cassandraConfiguration = ds.getContext().getBean( CassandraConfiguration.class );

        String hosts = cassandraConfiguration.getCassandraSeedNodes().stream().map( host -> host.getHostAddress() )
                .collect( Collectors.joining( "," ) );
        // TODO: Right now this test will only pass with in JVM spark master. For running on a spark cluster, you must
        // shadow build the kindling jar and make sure the path is set correctly.
        // Also idea does path with reference to super project so this will also fail in idea.

        conf = new SparkConf( true )
                .setMaster( "local[2]" )
                .setAppName( "Kindling" )
                .set( "spark.cassandra.connection.host", hosts )
                .set( "spark.cassandra.connection.port",
                        Integer.toString( 9042 ) );
        // .setJars( new String[] { "./kindling/build/libs/kindling-0.0.0-SNAPSHOT-all.jar" });
        spark = new SparkContext( conf );
        javaContext = new JavaSparkContext( spark );
        cassandraContext = new CassandraSQLContext( spark );
        cassandraJavaContext = javaFunctions( spark );
        authzManager = new SparkAuthorizationManager();
        csi = new ConductorSparkImpl( DatastoreConstants.KEYSPACE, javaContext, authzManager );
    }

}
