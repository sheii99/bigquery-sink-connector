package com.iblync.kafka.connect.bigquery.sink.converter;

import org.apache.kafka.connect.data.Schema;

public interface RecordConverter {
	
	/*
	 * Object Should be anything used as default BigDataJson Converter Or Anything
	 * like MAP,JSONObject etc...
	 */
	Object convert(Schema schema, Object value);
}
