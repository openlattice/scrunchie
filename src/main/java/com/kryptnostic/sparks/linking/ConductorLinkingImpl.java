package com.kryptnostic.sparks.linking;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dataloom.data.EntityKey;
import com.dataloom.datastore.services.SimpleElasticSearchBlocker;
import com.dataloom.datastore.services.SimpleHierarchicalClusterer;
import com.dataloom.datastore.services.SimpleMatcher;
import com.dataloom.datastore.services.SimpleMerger;
import com.dataloom.graph.GraphUtil;
import com.dataloom.graph.HazelcastLinkingGraphs;
import com.dataloom.graph.LinkingEdge;
import com.dataloom.linking.Entity;
import com.dataloom.linking.LinkingUtil;
import com.dataloom.linking.components.Blocker;
import com.dataloom.linking.components.Clusterer;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.components.Merger;
import com.dataloom.linking.util.UnorderedPair;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.Multimap;

public class ConductorLinkingImpl {
    
    private Blocker blocker;
    private Matcher matcher;
    private Clusterer clusterer;
    private HazelcastLinkingGraphs linkingGraph;
    
    public ConductorLinkingImpl( Blocker blocker, Matcher matcher, Clusterer clusterer, HazelcastLinkingGraphs linkingGraph ){
        this.blocker = blocker;
        this.matcher = matcher;
        this.clusterer = clusterer;
        this.linkingGraph = linkingGraph;
    }
    
    public UUID link( Map<UUID, UUID> entitySetsWithSyncIds, Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties ){
        initialize( entitySetsWithSyncIds, linkingMap, linkingProperties );
        //Create Linked Entity Set
        //TODO Fix
        UUID linkedEntitySetId = UUID.randomUUID();
        
        //Blocking: For each row in the entity sets turned dataframes, fire off query to elasticsearch
        Stream<UnorderedPair<Entity>> pairs = blocker.block();

        //Matching: check if pair score is already calculated, presumably from HazelcastGraph Api. If not, stream through matcher to get a score.
        pairs.filter( entityPair -> GraphUtil.isNewEdge( linkingGraph, linkedEntitySetId, LinkingUtil.getEntityKeyPair( entityPair ) ) )
        .forEach( entityPair -> {
            LinkingEdge edge = GraphUtil.linkingEdge( linkedEntitySetId, LinkingUtil.getEntityKeyPair( entityPair ) );
            
            double weight = matcher.score( entityPair );
            
            linkingGraph.addEdge( edge, weight );
        });

        //Feed the scores (i.e. the edge set) into HazelcastGraph Api
        
        /**
         * Got here right now.
         */
        
        //Clustering: once score calculation is complete, plug in the graph of entity keys to Spark PIC algorithm.
        Clusterer clusterer = new SimpleHierarchicalClusterer( scores.keySet(), (v,w) -> scores.get( new UnorderedPair(v,w) ) );
        
        //Merger: Clustering step should spit out Set<EntityKey>, the entities that are identified. Each entity goes through the merger to produce SetMultimap<String, Object>, ready to be passed to DataManager for storing.
        Merger merger = new SimpleMerger( linkingES, linkingProperties );
        
        StreamUtil.stream( clusterer.cluster() )
        .map( merger::merge )
        //TODO add a createEntityData method in CassandraDataManager that does not need to take datatype; by default uses jackson byte serialization.
        //TODO Perhaps add an extra step in the stream to batch process linkedEntities.
        .forEach( linkedEntity -> dataManager.createEntityData( entitySetId, syncId, linkedEntity ) );
        
        //Once data is finished merging/writing, return merged entity set id.
        
        return entitySetId;        
    }
    
    private void initialize( Map<UUID, UUID> entitySetsWithSyncIds, Multimap<UUID, UUID> linkingMap, Set<Map<UUID, UUID>> linkingProperties ){
        blocker.setLinking( entitySetsWithSyncIds, linkingMap );
        matcher.setLinking( linkingMap, linkingProperties );
        clusterer
    }

}
