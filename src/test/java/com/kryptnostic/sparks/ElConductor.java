package com.kryptnostic.sparks;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kryptnostic.conductor.rpc.Employee;

public class ElConductor {
    private static final Logger logger = LoggerFactory.getLogger( ElConductor.class );

    @Test
    public void localTest() {

        // TODO: Right now this test will only pass on my machine. Run it on your own local spark cluster by replacing
        // the master or setting master to "local"
        // Also idea does path with reference to super project so this will also fail in idea.
        SparkConf conf = new SparkConf().setAppName( "Kryptnostic Spark Datastore" )
                .setMaster( "local" );
        // .setJars( new String[] { "./kindling/build/libs/kindling-0.0.0-SNAPSHOT-all.jar" });
        JavaSparkContext spark = new JavaSparkContext( conf );
        JavaRDD<String> s = spark.textFile( "src/test/resources/employees.csv" );
        s.foreach( l -> System.out.println( l ) );
        JavaRDD<Employee> t = s.map( e -> Employee.EmployeeCsvReader.getEmployee( e ) );
        SQLContext context = new SQLContext( spark );
        logger.info( "Total # of employees: {}", t.count() );
        DataFrame df = context.createDataFrame( t, Employee.class );
        df.registerTempTable( "employees" );
        DataFrame emps = context.sql( "SELECT * from employees WHERE salary > 81500" );
        List<String> highlyPaidEmps = emps.javaRDD().map( e -> String.format( "%s,%s,%s,%d",
                e.getAs( "name" ),
                e.getAs( "dept" ),
                e.getAs( "title" ),
                e.getAs( "salary" ) ) ).collect();
        highlyPaidEmps.forEach( e -> logger.info( e ) );

        logger.info( "emps: {}", Lists.newArrayList( emps.javaRDD().map( e -> new Employee(
                e.getAs( "name" ),
                e.getAs( "dept" ),
                e.getAs( "title" ),
                (int) e.getAs( "salary" ) ) ).collect() ) );

    }
}
