package com.kryptnostic.sparks;

import java.util.List;

import javax.inject.Inject;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.kryptnostic.conductor.rpc.ConductorSparkApi;
import com.kryptnostic.conductor.rpc.Employee;

public class ConductorSparkImpl implements ConductorSparkApi {
    private static final long serialVersionUID = 4630183624909263596L;
    private static final Logger    logger = LoggerFactory.getLogger( ConductorSparkImpl.class );
    private final JavaSparkContext spark;

    @Inject
    public ConductorSparkImpl( JavaSparkContext spark ) {
        this.spark = spark;
    }

    @Override
    public List<Employee> processEmployees() {
        JavaRDD<Employee> t = spark.textFile( "kryptnostic-conductor/src/main/resources/employees.csv" )
                .map( e -> Employee.EmployeeCsvReader.getEmployee( e ) );
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

        return Lists.newArrayList( emps.javaRDD().map( e -> new Employee(
                e.getAs( "name" ),
                e.getAs( "dept" ),
                e.getAs( "title" ),
                (int) e.getAs( "salary" ) ) ).collect() );
    }
}
