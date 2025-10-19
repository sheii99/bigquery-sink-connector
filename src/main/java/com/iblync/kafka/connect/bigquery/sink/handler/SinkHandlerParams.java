package com.iblync.kafka.connect.bigquery.sink.handler;

import static java.util.Objects.requireNonNull;

import org.apache.kafka.connect.sink.SinkRecord;

import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;

public class SinkHandlerParams {

	  private final SinkRecord sinkRecord;
	  private final CDCMessage cdcMessage;
	  
	  public SinkHandlerParams(SinkRecord sinkRecord) {
		  this.sinkRecord = requireNonNull(sinkRecord);
		  this.cdcMessage = null;
	  }
	  
	  public SinkHandlerParams(SinkRecord sinkRecord, CDCMessage cdcMessage) {
		  this.sinkRecord = requireNonNull(sinkRecord);
		  this.cdcMessage = requireNonNull(cdcMessage);
	  }
	  
	  public SinkRecord sinkRecord() {
		    return sinkRecord;
		  }
	  
	  public CDCMessage getCdcMessage() {
		    return cdcMessage;
		  }
	  
	  public boolean hasCdcMessage() {
		    return cdcMessage != null;
		  }

}
