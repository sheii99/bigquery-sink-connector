package com.iblync.kafka.connect.bigquery.sink;

import static com.iblync.kafka.connect.bigquery.sink.BigQuerySinkTask.LOGGER;

import java.util.Collection;

import org.apache.kafka.connect.sink.SinkRecord;

public class StartedSinkTask implements AutoCloseable{
	
	StartedSinkTask(){
		
	}

	@Override
	public void close() throws Exception {
		
	}
	
	void put(final Collection<SinkRecord> records) {
		try {
			if (records.isEmpty()) {
				LOGGER.debug("No sink records to process for current poll operation");
				return;
			}
		}catch(Exception e) {
			LOGGER.error("Exception while processing record : ",e);
		}
	}

}
