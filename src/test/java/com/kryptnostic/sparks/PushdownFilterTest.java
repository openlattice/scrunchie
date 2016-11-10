package com.kryptnostic.sparks;

import static org.apache.spark.sql.functions.col;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import com.kryptnostic.conductor.rpc.odata.DatastoreConstants;
import com.kryptnostic.datastore.services.CassandraTableManager;

public class PushdownFilterTest extends BaseKindlingSparkTest {
    // TEST: testing pushdown for spark join
    @Test
    public void TestPushDown(){
        CassandraTableManager ctb = ds.getContext().getBean( CassandraTableManager.class );
        
        //Load DataSet from "Employees" Table
        Dataset<Row> dfEmployee = sparkSession
                .read()
                .format( "org.apache.spark.sql.cassandra" )
                .option( "table", ctb.getTablenameForEntityType( ENTITY_TYPE ) )
                .option( "keyspace", DatastoreConstants.KEYSPACE )
                .option( "pushdown", true )
                .load()
                .selectExpr( "entityid" );
        
        //Load DataSet from "Employees-DEPT" Table
        Dataset<Row> dfPropertyEmployeeDept = sparkSession
                .read()
                .format( "org.apache.spark.sql.cassandra" )
                .option( "table", ctb.getTablenameForPropertyValuesOfType( new FullQualifiedName( NAMESPACE, EMPLOYEE_DEPT ) ) )
                .option( "keyspace", DatastoreConstants.KEYSPACE )
                .option( "pushdown", true )
                .load()
                .select( "entityid", "value" )
                .where( "value = 'FIRE'");
        
        //Test 1.1: Did pushdown happen for the where equality clause
        System.out.println( "** Test 1.1: Did pushdown happen for where = clause in selecting rows in Cassandra Table turned DataFrame **" );
        System.err.println( "** Test 1.1: Did pushdown happen for where = clause in selecting rows in Cassandra Table turned DataFrame **" );
        dfPropertyEmployeeDept.explain();
        dfPropertyEmployeeDept.show();
        System.out.println( "** End of Test 1.1 **" );
        System.err.println( "** End of Test 1.1 **" );
        
        
        //Load DataSet from "Salary" Table
        Dataset<Row> dfPropertySalary = sparkSession
                .read()
                .format( "org.apache.spark.sql.cassandra" )
                .option( "table", ctb.getTablenameForPropertyValuesOfType( new FullQualifiedName( NAMESPACE, SALARY ) ) )
                .option( "keyspace", DatastoreConstants.KEYSPACE )
                .option( "pushdown", true )
                .load()
                .select( "entityid", "value" );
        
        Dataset<Row> dfPropertySalaryFilteredWhere = dfPropertySalary.where( "value > 80000");
        
        //Test 1.2: Did pushdown happen for the where > clause
        System.out.println( "** Test 1.2: Did pushdown happen for where > clause in selecting rows in Cassandra Table turned DataFrame **" );
        System.err.println( "** Test 1.2: Did pushdown happen for where > clause in selecting rows in Cassandra Table turned DataFrame **" );
        dfPropertySalaryFilteredWhere.explain();
        dfPropertySalaryFilteredWhere.show();
        System.out.println( "** End of Test 1.2 **" );
        System.err.println( "** End of Test 1.2 **" );

        Dataset<Row> dfPropertySalaryFilteredFilter = dfPropertySalary.filter( col("value").gt(80000) );
        //Test 1.3: Did pushdown happen for the filter clause
        System.out.println( "** Test 1.3: Did pushdown happen for filter clause in selecting rows in Cassandra Table turned DataFrame **" );
        System.err.println( "** Test 1.3: Did pushdown happen for filter clause in selecting rows in Cassandra Table turned DataFrame **" );
        dfPropertySalaryFilteredFilter.explain();
        dfPropertySalaryFilteredFilter.show();
        System.out.println( "** End of Test 1.3 **" );
        System.err.println( "** End of Test 1.3 **" );

        //Load DataSet from "Salary" Table, running sql query using spark
        dfPropertySalary.createOrReplaceTempView("salaryTableView");
        String query = "SELECT * from salaryTableView WHERE value > 80000";
        Dataset<Row> dfPropertySalaryFilteredSQL = sparkSession.sql( query );
        //Test 1.4: Did pushdown happen for running SQL query on a Cassandra Table turned Dataframe
        System.out.println( "** Test 1.4: Did pushdown happen for running SQL query on a Cassandra Table turned DataFrame **" );
        System.err.println( "** Test 1.4: Did pushdown happen for running SQL query on a Cassandra Table turned DataFrame **" );
        dfPropertySalaryFilteredSQL.explain();
        dfPropertySalaryFilteredSQL.show();
        System.out.println( "** End of Test 1.4 **" );
        System.err.println( "** End of Test 1.4 **" );
        
        //Do a join of all three data frames
        Dataset<Row> dfJoined = dfEmployee.join( dfPropertyEmployeeDept, "entityid").withColumnRenamed("value", "dept");
        dfJoined = dfJoined.join( dfPropertySalaryFilteredWhere, "entityid").withColumnRenamed( "value", "salary" );
        
        //Test 2: Did pushdown happen in spark join
        System.out.println( "** Test 2: Did pushdown happen for joining Cassandra Table turned DataFrame **" );
        System.err.println( "** Test 2: Did pushdown happen for joining Cassandra Table turned DataFrame **" );
        dfJoined.explain();
        dfJoined.show();
        System.out.println( "** End of Test 2 **" );
        System.err.println( "** End of Test 2 **" );
        
        dfJoined.createOrReplaceTempView("joinedTableView");
        String queryMultiple = "SELECT * from joinedTableView WHERE salary > 80000 AND dept = 'FIRE'";
        Dataset<Row> dfJoinedFilteredSQL = sparkSession.sql( queryMultiple );
        //Test 3.1: Did pushdown happen for running a multiple SQL query on a joined Cassandra Table
        System.out.println( "** Test 3.1: Did pushdown happen for running a multiple SQL query on a DataframeJoin of Cassandra Table **" );
        System.err.println( "** Test 3.1: Did pushdown happen for running a multiple SQL query on a DataframeJoin of Cassandra Table **" );
        dfJoinedFilteredSQL.explain();
        dfJoinedFilteredSQL.show();
        System.out.println( "** End of Test 3.1 **" );
        System.err.println( "** End of Test 3.1 **" );        
        
        dfJoined.createOrReplaceTempView("joinedTableView");
        String queryMultipleTwo = "SELECT * from joinedTableView WHERE salary < 150000 AND dept = 'FIRE'";
        Dataset<Row> dfJoinedFilteredSQLTwo = sparkSession.sql( queryMultipleTwo );
        //Test 3.1: Did pushdown happen for running a multiple SQL query on a joined Cassandra Table
        System.out.println( "** Test 3.2: Did pushdown happen for running a multiple SQL query on a DataframeJoin of Cassandra Table **" );
        System.err.println( "** Test 3.2: Did pushdown happen for running a multiple SQL query on a DataframeJoin of Cassandra Table **" );
        dfJoinedFilteredSQLTwo.explain();
        dfJoinedFilteredSQLTwo.show();
        System.out.println( "** End of Test 3.2 **" );
        System.err.println( "** End of Test 3.2 **" );    
    }
}
