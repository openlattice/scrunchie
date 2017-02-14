package com.kryptnostic.sparks.linking.components;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import com.dataloom.linking.Entity;
import com.dataloom.linking.components.Matcher;
import com.dataloom.linking.util.UnorderedPair;
import com.google.common.collect.Multimap;
import com.kryptnostic.datastore.services.EdmManager;

public class SimpleMatcher implements Matcher {

    private Multimap<UUID, UUID>         linkingMap;
    private Set<Map<UUID, UUID>>         linkingProperties;

    private Map<Map<UUID, UUID>, Double> weights;

    private final EdmManager             dms;

    public SimpleMatcher(
            EdmManager dms ) {
        this.dms = dms;
    }

    @Override
    //TODO implement distance calculator for "standard" features, such as names, that are captured in the getWeight method.
    public double score( UnorderedPair<Entity> entityPair ) {
        
    }

    @Override
    public void setLinking(
            Map<UUID, UUID> entitySetsWithSyncIds,
            Multimap<UUID, UUID> linkingMap,
            Set<Map<UUID, UUID>> linkingProperties ) {
        this.linkingMap = linkingMap;
        this.linkingProperties = linkingProperties;
        updateWeights();
    }

    private void updateWeights() {
        double totalWeight = 0;
        for ( Map<UUID, UUID> link : linkingProperties ) {
            double weight = getWeight( link );
            weights.put( link, weight );
            totalWeight += weight;
        }
        if ( totalWeight > 0 ) {
            normalizeWeights( totalWeight );
        } else {
            assignEqualWeights();
        }
    }

    private void normalizeWeights( double totalWeight ) {
        for ( Map<UUID, UUID> link : linkingProperties ) {
            double currentWeight = weights.get( link );
            weights.put( link, currentWeight / totalWeight );
        }
    }

    private void assignEqualWeights() {
        double weight = 1D / linkingProperties.size();
        for ( Map<UUID, UUID> link : linkingProperties ) {
            weights.put( link, weight );
        }
    }

    // TODO lolz
    // Right now, 10 is something with heavy weight, 1 is smallest.
    private double getWeight( Map<UUID, UUID> link ) {
        Set<String> propertyNames = dms.getPropertyTypes( new HashSet<>( link.values() ) ).stream()
                .map( pt -> pt.getType().getName() ).collect( Collectors.toSet() );
        if ( propertyNames.contains( "year" ) || propertyNames.contains( "date" ) || propertyNames.contains( "dob" )
                || propertyNames.contains( "id" ) || propertyNames.contains( "ssn" ) ) {
            return 10;
        } else if ( propertyNames.contains( "name" ) || propertyNames.contains( "firstname" )
                || propertyNames.contains( "lastname" ) ) {
            return 7;
        }
        return 0;
    }
}
