package com.kryptnostic.sparks;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dataloom.hazelcast.HazelcastMap;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.kryptnostic.conductor.rpc.OrderedRPCKey;
import com.kryptnostic.rhizome.core.Cutting;

public final class SparkTransforms implements Serializable {
    private static final long   serialVersionUID = 3936793086983156958L;

    private static final Logger logger           = LoggerFactory.getLogger( SparkTransforms.class );

    private SparkTransforms() {}

    public static VoidFunction<Row> saveTopUtilizers(
            Cutting cutting,
            List<UUID> columnIdsOrdered,
            UUID requestId,
            UUID propertyTypeId ) {
        return row -> {
            HazelcastInstance hazelcastBean;
            try {
                hazelcastBean = cutting.getBean( HazelcastInstance.class );
                IMap<OrderedRPCKey, byte[]> rpcMap = hazelcastBean
                        .getMap( HazelcastMap.RPC_DATA_ORDERED.name() );
                SetMultimap<UUID, Object> results = HashMultimap.create();
                for ( int i = 0; i < columnIdsOrdered.size(); i++ ) {
                    if ( row.isNullAt( i ) ) {
                        results.put( columnIdsOrdered.get( i ), Lists.newArrayList() );
                    } else {
                        results.putAll( columnIdsOrdered.get( i ), row.getList( i ) );
                    }
                }
                rpcMap
                        .set( new OrderedRPCKey(
                                requestId,
                                results.get( propertyTypeId ).size() * 1.0 ),
                                ObjectMappers.getSmileMapper().writeValueAsBytes( results ) );

            } catch ( InterruptedException | JsonProcessingException e ) {
                logger.debug( "an error ocurred when trying to write data" );
            }

        };
    }

}
