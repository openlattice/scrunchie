/*
 * Copyright (C) 2017. Kryptnostic, Inc (dba Loom)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@thedataloom.com
 */

package com.kryptnostic.sparks;

import com.datastax.driver.core.DataType;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

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
