package com.kryptnostic.sparks;

import java.util.Arrays;
import java.util.UUID;

import com.datastax.spark.connector.writer.RowWriter;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class RowWriterForUUID implements RowWriter<UUID> {
    private static final long serialVersionUID = -8740718057877877922L;

    @Override
    public Seq<String> columnNames() {
        return JavaConversions
                .asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                .toList();
    }

    @Override
    public void readColumnValues( UUID entityId, Object[] arg1 ) {
        arg1[ 0 ] = entityId;
    }
}
