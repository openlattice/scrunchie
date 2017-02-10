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

import com.datastax.spark.connector.writer.RowWriter;
import org.apache.spark.sql.Row;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.List;

public class CacheTableRowWriter implements RowWriter<Row> {
    private static final long serialVersionUID = -937664101639772865L;
    private final List<String> columnNames;

    public CacheTableRowWriter( List<String> columnNames ) {
        this.columnNames = columnNames;
    }

    @Override
    public Seq<String> columnNames() {
        return JavaConversions.asScalaBuffer( columnNames );
    }

    @Override
    public void readColumnValues( Row data, Object[] buffer ) {
        for ( int i = 0, j = 0; i < columnNames.size(); i++, j++ ) {
            buffer[ i ] = data.get( j );
        }

    }
}
