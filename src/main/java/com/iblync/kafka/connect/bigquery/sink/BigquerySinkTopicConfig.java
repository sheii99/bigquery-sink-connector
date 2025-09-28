package com.iblync.kafka.connect.bigquery.sink;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.sink.config.KafkaConfigProxyFactory;

public class BigquerySinkTopicConfig {
	
	private static final String prefix = "bigquery.";
	private static final KafkaConfigProxyFactory factory = new KafkaConfigProxyFactory(prefix);

	public static <T> BigQueryConfig parse(Map<String, String> props) {
	    return factory.newProxy(BigQueryConfig.class, props);
	  }
	
	  public static ConfigDef define(Class<?> configClass) {
		    return factory.define(configClass);
		  }


}
