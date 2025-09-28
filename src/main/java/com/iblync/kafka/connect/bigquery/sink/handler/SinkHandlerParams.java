package com.iblync.kafka.connect.bigquery.sink.handler;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.connect.sink.SinkRecord;

public class SinkHandlerParams {

	  private final SinkRecord sinkRecord;
	  
	  public SinkHandlerParams( SinkRecord sinkRecord) {
		  this.sinkRecord = requireNonNull(sinkRecord);
	  }
	  
	  public SinkRecord sinkRecord() { // 
		    return sinkRecord;
		  }

}
