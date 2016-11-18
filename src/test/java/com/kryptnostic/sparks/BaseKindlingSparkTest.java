package com.kryptnostic.sparks;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.edm.internal.DatastoreConstants;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.hazelcast.core.HazelcastInstance;
import com.kryptnostic.datastore.edm.BootstrapDatastoreWithCassandra;
import com.kryptnostic.datastore.services.ActionAuthorizationService;
import com.kryptnostic.datastore.services.CassandraTableManager;
import com.kryptnostic.datastore.services.EdmManager;
import com.kryptnostic.rhizome.configuration.cassandra.CassandraConfiguration;
import com.kryptnostic.rhizome.pods.SparkPod;

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
    static {
        PROFILES.add( SparkPod.SPARK_PROFILE );
        PODS.add( SparkPod.class );
        LoomCassandraConnectionFactory.configureSparkPod();
    }

    @BeforeClass
    public static void initSpark() {
        ds.intercrop( SparkPod.class );
        ds.getContext().getEnvironment().addActiveProfile( SparkPod.SPARK_PROFILE );
        init();
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        EdmManager edm = ds.getContext().getBean( EdmManager.class );
        Assert.assertTrue( "SSL must be enabled",
                ds.getContext().getBean( CassandraConfiguration.class ).isSslEnabled() );
        sparkSession = ds.getContext().getBean( SparkSession.class );
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
                ds.getContext().getBean( HazelcastInstance.class ) );
    }

}
