package com.iblync.kafka.connect.bigquery.sink.handler;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import java.util.Map;

import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;

public class SinkHandlerContext {// pass the config properties, bigquery config 
	private final Map<String, String> configProperties;
	private final BigQueryConfig config;// getter and setter 
	
	
	/* Configure Property like connections configrations */
	public SinkHandlerContext(Map<String, String> configProperties, BigQueryConfig config) {// bigquery config 
		this.configProperties = requireNonNull(unmodifiableMap(configProperties));
		this.config = config; 
	}

	public Map<String, String> configProperties() {
		return configProperties;
	}
	
	public BigQueryConfig config(){
		return config;
	}

}
