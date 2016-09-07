package com.kryptnostic.sparks;

import com.datastax.spark.connector.writer.RowWriter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import java.util.List;

/**
 * Created by yao on 9/7/16.
 */
public class CacheTableRowWriter implements RowWriter<Row> {
    private final List<String> columnNames;

    public CacheTableRowWriter(List<String> columnNames){
        this.columnNames = columnNames;
    }

    @Override public Seq<String> columnNames() {
        return scala.collection.JavaConversions.asScalaBuffer( columnNames );
    }

    @Override public void readColumnValues( Row data, Object[] buffer ) {
        int i = 0;
        for (String columnName : columnNames ){
            buffer[i] = data.getAs( columnName );
        }
    }
}
