package com.iblync.kafka.connect.bigquery.sink.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class StringRecordConverter  implements RecordConverter {

	  @Override
	  public Object convert(final Schema schema, final Object value) {
	    if (value == null) {
	      throw new DataException("Value was null for JSON conversion");
	    }

	    return value.toString();
	  }
}
