package com.kryptnostic.kindling.search;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.kryptnostic.instrumentation.v1.constants.InstrumentationConstants;
import com.kryptnostic.rhizome.configuration.Configuration;
import com.kryptnostic.rhizome.configuration.ConfigurationKey;
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey;

public class KindlingConfiguration implements Configuration {
	
	private static final long serialVersionUID = 1997194565743699166L;
	private static final ConfigurationKey key = new SimpleConfigurationKey("kindling.yaml");
	private final Optional<String> elasticsearchUrl;
	private final Optional<String> elasticsearchCluster;
	
	public KindlingConfiguration(
			@JsonProperty( InstrumentationConstants.ELASTICSEARCH_URL ) Optional<String> elasticsearchUrl,
			@JsonProperty( InstrumentationConstants.ELASTICSEARCH_CLUSTER ) Optional<String> elasticsearchCluster ) {
		this.elasticsearchUrl = elasticsearchUrl;
		this.elasticsearchCluster = elasticsearchCluster;
	}
	
	@JsonProperty( InstrumentationConstants.ELASTICSEARCH_URL )
	public Optional<String> getElasticsearchUrl() {
		return elasticsearchUrl;
	}
	
	@JsonProperty( InstrumentationConstants.ELASTICSEARCH_CLUSTER )
	public Optional<String> getElasticsearchCluster() {
		return elasticsearchCluster;
	}

	@Override
	public ConfigurationKey getKey() {
		return key;
	}

}
