package com.kryptnostic.sparks.linking.components;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;

import com.dataloom.data.EntityKey;
import com.dataloom.datastore.services.CassandraDataManager;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.kryptnostic.conductor.rpc.ConductorElasticsearchApi;
import com.kryptnostic.datastore.services.EdmManager;

public class SimpleElasticSearchBlocker implements Blocker {

    private final SparkSession              sparkSession;
    private final SparkContextJavaFunctions cassandraJavaContext;
    private final EdmManager                dms;
    private final CassandraDataManager      dataManager;
    private final ConductorElasticsearchApi elasticsearchApi;

    private Map<UUID, UUID>                 entitySetsWithSyncIds;
    private Multimap<UUID, UUID>            linkingMap;
    private Set<UUID>                       linkingES;

    // Number of search results taken in each block.
    private int                             blockSize = 50;
    // Whether explanation for search results is stored.
    private boolean                         explain   = false;

    public SimpleElasticSearchBlocker(
            SparkSession sparkSession,
            SparkContextJavaFunctions cassandraJavaContext,
            EdmManager dms,
            CassandraDataManager dataManager,
            ConductorElasticsearchApi elasticsearchApi ) {
        this.sparkSession = sparkSession;
        this.cassandraJavaContext = cassandraJavaContext;
        this.dataManager = dataManager;
        this.dms = dms;
        this.elasticsearchApi = elasticsearchApi;

        this.sparkSession.sql( "set spark.sql.caseSensitive=false" );
    }

    @Override
    public void setLinking( Map<UUID, UUID> entitySetsWithSyncIds, Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties ) {
        this.entitySetsWithSyncIds = entitySetsWithSyncIds;
        this.linkingMap = linkingMap;
        this.linkingES = entitySetsWithSyncIds.keySet();
    }

    @Override
    public Stream<UnorderedPair<Entity>> block() {
        return loadEntitySets();
    }

    private Stream<UnorderedPair<Entity>> loadEntitySets() {
        return entitySetsWithSyncIds.keySet().stream().flatMap( this::loadEntitySet );
    }

    private Stream<UnorderedPair<Entity>> loadEntitySet( UUID entitySetId ) {
        // TODO update syncIds once entity key is updated.
        Set<UUID> propertiesSet = ImmutableSet.copyOf( linkingMap.get( entitySetId ) );
        Map<UUID, PropertyType> propertiesMap = propertiesSet.stream()
                .collect( Collectors.toMap( ptId -> ptId, ptId -> dms.getPropertyType( ptId ) ) );
        Set<UUID> syncIds = ImmutableSet.of( entitySetsWithSyncIds.get( entitySetId ) );
        Iterable<String> entityIds = dataManager.getEntityIds( entitySetId, syncIds );
        return StreamUtil.stream( entityIds )
                .map( entityId -> new EntityKey( entitySetsWithSyncIds.get( entitySetId ), entitySetId, entityId ) )
                .map( key -> Pair.of( key, dataManager.asyncLoadEntity( key.getEntityId(), syncIds, propertiesSet ) ) )
                // .map( rsfPair -> mapPair( rsfPair, ResultSetFuture::getUninterruptibly ) )
                .map( rsfPair -> Pair.of( rsfPair.getKey(), rsfPair.getValue().getUninterruptibly() ) )
                .map( rsPair -> Pair.of( rsPair.getKey(),
                        dataManager.rowToEntityIndexedById( rsPair.getValue(), propertiesMap ) ) )
                .flatMap( entityPair -> {
                    // TODO fix the uber terrible toString later - this should be made into an OR query
                    Map<UUID, Set<String>> properties = propertiesSet.stream()
                            .collect( Collectors.toMap( ptId -> ptId,
                                    ptId -> entityPair.getValue().get( ptId ).stream().map( obj -> obj.toString() )
                                            .collect( Collectors.toSet() ) ) );

                    
                    Map<String, Object> propertiesWithStringIndex = properties.entrySet().stream().collect( Collectors.toMap( entry -> entry.getKey().toString(), entry -> entry.getValue() ) );
                    Entity currentEntity = new Entity( entityPair.getKey(), propertiesWithStringIndex );
                    
                    List<Entity> queryResults = elasticsearchApi.executeEntitySetDataSearchAcrossIndices( linkingES,
                            properties,
                            blockSize,
                            explain );

                    // return entity key
                    return queryResults.stream().map( entity -> new UnorderedPair<Entity>( currentEntity, entity ) );
                } );
    }

    private <R, S, T> Pair<R, T> mapPair( Pair<R, S> pair, Function<S, T> transform ) {
        return Pair.of( pair.getKey(), transform.apply( pair.getValue() ) );
    }
}
