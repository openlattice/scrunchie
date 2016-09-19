package com.kryptnostic.sparks;

import com.datastax.spark.connector.writer.RowWriter;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;

/**
 * Created by yao on 9/7/16.
 */
public class CacheTableRowWriter implements RowWriter<Row> {
    private static final long serialVersionUID = -937664101639772865L;
    private final List<String> columnNames;

    public CacheTableRowWriter( List<String> columnNames ) {
        this.columnNames = columnNames;
    }

    @Override
    public Seq<String> columnNames() {
        return JavaConversions.asScalaBuffer( columnNames );
    }

    @Override
    public void readColumnValues( Row data, Object[] buffer ) {
        for ( int i = 0, j = 0; i < columnNames.size(); i++, j++ ) {
            buffer[ i ] = data.get( j );
        }

    }
}
