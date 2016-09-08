package com.kryptnostic.sparks;

import com.datastax.spark.connector.writer.RowWriter;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;
import scala.collection.Seq;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by yao on 9/7/16.
 */
public class CacheTableRowWriter implements RowWriter<Row> {
    private final List<String> fqNames;
    private final List<String> columnNames;

    public CacheTableRowWriter(List<FullQualifiedName> fqnList){
        this.fqNames = fqnList.stream().map( fqn -> fqn.getFullQualifiedNameAsString() ).collect( Collectors.toList());
        this.columnNames = fqnList.stream().map( fqn -> fqn.getName() ).collect( Collectors.toList());
    }

    @Override public Seq<String> columnNames() {
        System.err.println(columnNames.toString());
        return scala.collection.JavaConversions.asScalaBuffer( columnNames );
    }

    @Override public void readColumnValues( Row data, Object[] buffer ) {
        int i = 0;
        int j = 1;
        buffer[i++] = data.get( j++ );
        buffer[i++] = data.get( j++ );
        buffer[i++] = data.get( j++ );
        buffer[i++] = data.get( j++ );
        buffer[i] = data.get( j );
    }
}
