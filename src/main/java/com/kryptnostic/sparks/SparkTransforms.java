package com.kryptnostic.sparks;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.mappers.ObjectMappers;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.kryptnostic.conductor.rpc.odata.Table;
import com.kryptnostic.datastore.cassandra.CommonColumns;
import com.kryptnostic.rhizome.core.Cutting;

public final class SparkTransforms implements Serializable {
    private static final long   serialVersionUID = 3936793086983156958L;

    private static final Logger logger           = LoggerFactory.getLogger( SparkTransforms.class );

    private SparkTransforms() {}

    public static VoidFunction<Row> saveTopUtilizers(
            Cutting cutting,
            List<UUID> columnIdsOrdered,
            UUID requestId,
            Set<UUID> propertyTypeIds ) {
        return row -> {
            try {
                Session session = cutting.getBean( Session.class );
                SetMultimap<UUID, Object> results = HashMultimap.create();
                for ( int i = 0; i < columnIdsOrdered.size(); i++ ) {
                    if ( row.isNullAt( i ) ) {
                        results.put( columnIdsOrdered.get( i ), Lists.newArrayList() );
                    } else {
                        results.putAll( columnIdsOrdered.get( i ), row.getList( i ) );
                    }
                }
                double weight = 0;
                for ( UUID propertyTypeId : propertyTypeIds ) {
                    weight += results.get( propertyTypeId ).size() * 1.0;
                }
                session.execute( QueryBuilder
                        .insertInto( Table.RPC_DATA_ORDERED.getKeyspace(), Table.RPC_DATA_ORDERED.getName() )
                        .ifNotExists()
                        .value( CommonColumns.RPC_REQUEST_ID.cql(), requestId )
                        .value( CommonColumns.RPC_WEIGHT.cql(), weight )
                        .value( CommonColumns.RPC_VALUE.cql(),
                                ByteBuffer.wrap( ObjectMappers.getSmileMapper().writeValueAsBytes( results ) ) ) );
            } catch ( InterruptedException | JsonProcessingException e ) {
                logger.debug( "an error ocurred when trying to write data" );
            }

        };
    }

}
