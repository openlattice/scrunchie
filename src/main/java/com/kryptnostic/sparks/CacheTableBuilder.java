package com.kryptnostic.sparks;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Created by yao on 9/6/16.
 */
public class CacheTableBuilder {
    private final String name;
    private List<String> columnNames;
    private List<DataType> dataTypes;

    public CacheTableBuilder(String name){
        Preconditions.checkArgument( StringUtils.isNotBlank( name ), "Table name cannot be blank." );
        this.name = name;
    }

    public CacheTableBuilder columns( List<String> columnNames, List<DataType> dataTypes ){
        Preconditions.checkNotNull( columnNames );
        Preconditions.checkNotNull( dataTypes );
        Preconditions.checkState( columnNames.size() == dataTypes.size() );
        columnNames.forEach( Preconditions::checkNotNull );
        dataTypes.forEach( Preconditions::checkNotNull );
        this.columnNames = columnNames;
        this.dataTypes = dataTypes;
        return this;
    }

    public String buildQuery() {
        StringBuilder query = new StringBuilder( "CREATE TABLE cache." ).append( name );
        query.append( " ( " );
        for(int i = 0; i < columnNames.size(); i++){
            query.append( columnNames.get( i ) )
                    .append( " " )
                    .append( dataTypes.get( i ).toString() )
                    .append( ", " );
        }
        query.append( "PRIMARY KEY ( entityid ) )" );

        return query.toString();
    }

}
