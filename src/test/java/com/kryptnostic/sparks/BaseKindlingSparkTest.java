package com.kryptnostic.sparks;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.edm.BootstrapDatastoreWithCassandra;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;

public class BaseKindlingSparkTest extends BootstrapDatastoreWithCassandra {
    // Need to start Cassandra/Spark/Datastore
    protected static SparkConf                  conf;
    protected static SparkSession               sparkSession;
    protected static JavaSparkContext           javaContext;
    protected static SQLContext                 cassandraContext;
    protected static SparkContextJavaFunctions  cassandraJavaContext;
    protected static SparkAuthorizationManager  authzManager;
    protected static ActionAuthorizationService authzService;
    protected static ConductorSparkImpl         csi;
    protected final Logger                      logger = LoggerFactory.getLogger( getClass() );

    @BeforeClass
    public static void initSpark() {
        CassandraConfiguration cassandraConfiguration = ds.getContext().getBean( CassandraConfiguration.class );
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        EdmManager edm = ds.getContext().getBean( EdmManager.class );
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
        sparkSession = SparkSession.builder().config( conf ).getOrCreate();
        javaContext = new JavaSparkContext( sparkSession.sparkContext() );
        cassandraContext = new SQLContext( sparkSession.sparkContext() );
        cassandraJavaContext = javaFunctions( sparkSession.sparkContext() );
        authzManager = new SparkAuthorizationManager();
        authzService = ds.getContext().getBean( ActionAuthorizationService.class );
        csi = new ConductorSparkImpl(
                DatastoreConstants.KEYSPACE,
                sparkSession,
                cassandraJavaContext,
                ctb,
                edm,
                authzManager,
                authzService );
    }

}
