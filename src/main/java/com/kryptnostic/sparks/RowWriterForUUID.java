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

import java.util.Arrays;
import java.util.UUID;

import com.datastax.spark.connector.writer.RowWriter;
import com.kryptnostic.datastore.cassandra.CommonColumns;

import scala.collection.JavaConversions;
import scala.collection.Seq;

public class RowWriterForUUID implements RowWriter<UUID> {
    private static final long serialVersionUID = -8740718057877877922L;

    @Override
    public Seq<String> columnNames() {
        return JavaConversions
                .asScalaBuffer( Arrays.asList( CommonColumns.ENTITYID.cql() ) )
                .toList();
    }

    @Override
    public void readColumnValues( UUID entityId, Object[] arg1 ) {
        arg1[ 0 ] = entityId;
    }
}
