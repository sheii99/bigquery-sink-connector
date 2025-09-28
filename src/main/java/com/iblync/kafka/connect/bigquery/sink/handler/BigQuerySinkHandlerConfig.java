package com.iblync.kafka.connect.bigquery.sink.handler;

import java.util.List;

import com.iblync.kafka.connect.bigquery.util.Default;

public interface BigQuerySinkHandlerConfig { //topic level
 // add json properties 
	@Default
	List<String> topicToTable();
	// camel case will converted into small case.  to match the json
	// it will return raw string we have to convert the string into topic based 
	// for that we're going to use topic to map class.
	 
	@Default
    List<String> topicToDataset();
    
	@Default
    List<String> topicToOperations();
	
	// if want add the other properties 
	@Default
	List<String> topicToPk();// after the dot is c
	
	

}
