package com.iblync.kafka.connect.bigquery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import com.iblync.kafka.connect.bigquery.sink.BigQuerySinkTask;
import com.iblync.kafka.connect.bigquery.sink.BigquerySinkTopicConfig;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.util.Versions;

public class BigQuerySinkConnector extends SinkConnector {

	private Map<String, String> properties;

	@Override
	public String version() {
		return Versions.getVersion();
	}

	@Override // here we map the properties 
	public void start(Map<String, String> map) {
		properties = map;
	}

	@Override
	public Class<? extends Task> taskClass() {
		return BigQuerySinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(properties);
		}
		return taskConfigs;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
	}

	@Override
	public ConfigDef config() {
		return BigquerySinkTopicConfig.define(BigQueryConfig.class);
	}

}
