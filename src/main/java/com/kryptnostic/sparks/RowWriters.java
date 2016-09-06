package com.kryptnostic.sparks;

import java.util.UUID;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.StructDef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.mapper.ColumnMapForReading;
import com.datastax.spark.connector.mapper.ColumnMapForWriting;
import com.datastax.spark.connector.mapper.ColumnMapper;
import com.datastax.spark.connector.util.JavaApiHelper;
import com.datastax.spark.connector.writer.DefaultRowWriter;
import com.datastax.spark.connector.writer.RowWriter;

import scala.collection.immutable.IndexedSeq;

public class RowWriters {
//    RowWriter<UUID> entityIdRowWriter( TableDef t, IndexedSeq<ColumnRef> selectedColumns ) {
//        return new DefaultRowWriter<UUID>(
//                t,
//                selectedColumns,
//                JavaApiHelper.getTypeTag( UUID.class ), CassandraJavaUtil.col
//        new ColumnMapper<UUID>() {
//
//            @Override
//            public ColumnMapForReading columnMapForReading(
//                    StructDef arg0,
//                    scala.collection.IndexedSeq<ColumnRef> arg1 ) {
//                // TODO Auto-generated method stub
//                return null;
//            }
//
//            @Override
//            public ColumnMapForWriting columnMapForWriting(
//                    StructDef arg0,
//                    scala.collection.IndexedSeq<ColumnRef> arg1 ) {
//                // TODO Auto-generated method stub
//                return null;
//            }
//
//            @Override
//            public TableDef newTable( String arg0, String arg1 ) {
//                // TODO Auto-generated method stub
//                return null;
//            }
//        } );
//    }
}
