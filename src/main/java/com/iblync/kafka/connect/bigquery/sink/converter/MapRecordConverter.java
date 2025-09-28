package com.iblync.kafka.connect.bigquery.sink.converter;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class MapRecordConverter implements RecordConverter {

	  @SuppressWarnings({"unchecked"})
	  @Override
	  public Object convert(final Schema schema, final Object value) {
	    if (value == null) {
	      throw new DataException("Value was null for JSON conversion");
	    }
	    return (Map<String, Object>) value;
	  }
}
