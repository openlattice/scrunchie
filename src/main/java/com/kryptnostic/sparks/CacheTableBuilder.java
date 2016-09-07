package com.kryptnostic.sparks;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by yao on 9/6/16.
 */
public class CacheTableBuilder {
    private final String name;
    private Map<String, DataType> columns = Maps.newHashMap();

    public CacheTableBuilder(String name){
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Table name cannot be blank." );
        this.name = name;
    }

    public CacheTableBuilder columns( Map<String, DataType> columnNameToType){
        Preconditions.checkNotNull( columnNameToType );
        Preconditions.checkState( columnNameToType.size() > 0 );
        this.columns = columnNameToType;
        Arrays.asList(columns).forEach( Preconditions::checkNotNull );
        return this;
    }

    public String buildQuery() {
        StringBuilder query = new StringBuilder( "CREATE TABLE cache." ).append( name );
        query.append( " ( " );
        if( columns.size() > 0 ){
            appendColumnDefs( query, columns );
        }
        query.append( "PRIMARY KEY ( objectid ) )" );

        return query.toString();
    }

    private StringBuilder appendColumnDefs( StringBuilder query, Map<String, DataType> columns ) {
        columns.entrySet().stream().forEach( e -> query.append( e.getKey() ).append( " " ).append( e.getValue().toString() ).append( ", " ) );
        return query;
    }

}
