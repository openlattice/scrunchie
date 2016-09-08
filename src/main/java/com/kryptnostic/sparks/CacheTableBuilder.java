package com.kryptnostic.sparks;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by yao on 9/6/16.
 */
public class CacheTableBuilder {
    private final String name;
    private List<FullQualifiedName> fullQualifiedNames;
    private List<DataType> dataTypes;

    public CacheTableBuilder(String name){
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Table name cannot be blank." );
        this.name = name;
    }

    public CacheTableBuilder columns( List<FullQualifiedName> fullQualifiedNames, List<DataType> dataTypes ){
//        Preconditions.checkNotNull( columnNameToType );
//        Preconditions.checkState( columnNameToType.size() > 0 );
        this.fullQualifiedNames = fullQualifiedNames;
        this.dataTypes = dataTypes;
//        Arrays.asList(columns).forEach( Preconditions::checkNotNull );
        return this;
    }

    public String buildQuery() {
        StringBuilder query = new StringBuilder( "CREATE TABLE cache." ).append( name );
        query.append( " ( " );
        for(int i = 0; i < fullQualifiedNames.size(); i++){
            query.append( fullQualifiedNames.get( i ).getName() )
                    .append( " " )
                    .append( dataTypes.get( i ).toString() )
                    .append( ", " );
        }
        query.append( "PRIMARY KEY ( entityid ) )" );

        return query.toString();
    }

    private StringBuilder appendColumnDefs( StringBuilder query, Map<FullQualifiedName, DataType> columns ) {
        columns.entrySet().stream().forEach( e -> query.append( e.getKey().getName() ).append( " " ).append( e.getValue().toString() ).append( ", " ) );
        return query;
    }

}
