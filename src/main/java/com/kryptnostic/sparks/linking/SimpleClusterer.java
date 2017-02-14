package com.kryptnostic.sparks.linking;

import java.util.UUID;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dataloom.linking.components.Clusterer;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.kryptnostic.conductor.rpc.odata.Tables;
import com.kryptnostic.datastore.cassandra.CommonColumns;

public class SimpleClusterer implements Clusterer {

    private final SparkSession              sparkSession;
    private final SparkContextJavaFunctions cassandraJavaContext;

    private UUID                            linkedEntitySetId;

    public SimpleClusterer( SparkSession sparkSession, SparkContextJavaFunctions cassandraJavaContext ) {
        this.sparkSession = sparkSession;
        this.cassandraJavaContext = cassandraJavaContext;
    }

    @Override
    public void setId( UUID linkedEntitySetId ) {
        this.linkedEntitySetId = linkedEntitySetId;
    }

    @Override
    public void cluster() {
        Dataset<Row> graphDf = sparkSession
                .read()
                .format( "org.apache.spark.sql.cassandra" )
                .option( "table", Tables.LINKING_EDGES.getName().toLowerCase() )
                .option( "keyspace", Tables.LINKING_EDGES.getKeyspace() )
                .load();        
        
    }

}
