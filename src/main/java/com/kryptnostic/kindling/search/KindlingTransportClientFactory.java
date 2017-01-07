package com.kryptnostic.kindling.search;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public class KindlingTransportClientFactory {
	
	public static final Logger log = LoggerFactory.getLogger( KindlingTransportClientFactory.class );
	private String clientTransportHost;
	private Integer clientTransportPort;
	private boolean disabled = false;
	private String cluster;
	
	public KindlingTransportClientFactory(
			String clientTransportHost,
			Integer clientTransportPort,
			boolean disabled,
			String cluster ) {
		this.clientTransportHost = clientTransportHost;
		this.clientTransportPort = clientTransportPort;
		this.disabled = disabled;
		this.cluster = cluster;
	}
	
	public Client getClient() throws UnknownHostException {
		if ( this.disabled ) {
			log.info( "logging to elasticsearch disabled ");
			return null;
		} else if ( this.clientTransportHost == null ) {
			log.info( "no server passed in, logging to database" );
			return null;
		}
		
		
		log.info( "getting kindling elasticsearch client on " + clientTransportHost + ":" + clientTransportPort + " with elasticsearch cluster " + cluster );
		Settings settings = Settings.builder().put( "cluster.name", cluster ).build();
		TransportClient client = new PreBuiltTransportClient( settings );
		client.addTransportAddress( new InetSocketTransportAddress(
				InetAddress.getByName( this.clientTransportHost ),
				this.clientTransportPort )
		);
		
		if ( verifyConnection( client ) ) {
			return client;
		} else {
			return null;
		}
	}
	
	public boolean verifyConnection( Client someClient ) {
		if ( someClient == null ) {
			log.info( "not connected to elasticsearch" );
			return false;
		} else if ( someClient instanceof TransportClient ) {
			TransportClient client = (TransportClient) someClient;
			List<DiscoveryNode> nodes = client.connectedNodes();
			if ( nodes.isEmpty() ) {
				log.info( "no elasticsearch nodes found" );
				client.close();
				return false;
			} else {
				log.info( "connected to elasticsearch nodes: " + nodes.toString() );
				return true;
			}
		} else {
			NodeClient client = (NodeClient) someClient;
			ClusterStateRequest request = new ClusterStateRequest();
			Future<ClusterStateResponse> response = client.admin().cluster().state( request );
			try {
				response.get();
				log.info( "connected to elasticsearch" );
				return true;
			} catch ( InterruptedException | ExecutionException e ) {
				log.info( "not connected to elasticsearch" );
				client.close();
				return false;
			}
		}
	}
	
}
