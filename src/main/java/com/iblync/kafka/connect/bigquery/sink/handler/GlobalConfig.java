package com.iblync.kafka.connect.bigquery.sink.handler;

import com.iblync.kafka.connect.bigquery.util.Default;

public interface GlobalConfig { // not topic based 
	@Default("")
	String dataset();

	@Default("3")
	int maxRetries();
	
	@Default("UPDATE")
	String operations(); // CUD 
	
	@Default
	String project(); 
	
}
