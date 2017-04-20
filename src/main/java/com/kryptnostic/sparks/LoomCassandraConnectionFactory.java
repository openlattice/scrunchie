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

import com.datastax.driver.core.Cluster;
import com.datastax.spark.connector.cql.CassandraConnectionFactory;
import com.datastax.spark.connector.cql.CassandraConnectorConf;
import com.datastax.spark.connector.cql.Scanner;
import com.datastax.spark.connector.rdd.ReadConf;
import com.google.common.base.Preconditions;
import com.kryptnostic.rhizome.pods.SparkPod;

import scala.collection.IndexedSeq;
import scala.collection.immutable.Set;

public class LoomCassandraConnectionFactory implements CassandraConnectionFactory {
    private static final long                 serialVersionUID = 4167984603461650295L;
    private static final ThreadLocal<Cluster> cluster          = new ThreadLocal<Cluster>() {
                                                                   @Override
                                                                   protected Cluster initialValue() {
                                                                       return SparkPod.getCluster().get();
                                                                   }
                                                               };

    static {
        configureSparkPod();
    }

    @Override
    public Cluster createCluster( CassandraConnectorConf ccf ) {
        Cluster c = cluster.get();
        if ( c.isClosed() ) {
            c = SparkPod.getCluster().get();
            cluster.set( c );
        }
        return Preconditions.checkNotNull( c,
                "This is a hack that must be initialized before being used in a session using the CassandraPod" );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public Set<String> properties() {
        return (Set<String>) scala.collection.immutable.Set$.MODULE$.empty();
    }

    @Override public Scanner getScanner(
            ReadConf readConf, CassandraConnectorConf cassandraConnectorConf, IndexedSeq<String> indexedSeq ) {
        return null;
    }

    public static void configureSparkPod() {
        SparkPod.CASSANDRA_CONNECTION_FACTORY_CLASS = LoomCassandraConnectionFactory.class.getCanonicalName();
    }

}
