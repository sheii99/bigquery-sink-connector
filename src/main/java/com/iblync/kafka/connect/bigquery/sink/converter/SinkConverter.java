package com.iblync.kafka.connect.bigquery.sink.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SinkConverter {

	@SuppressWarnings("unchecked")
	public Map<String,Object> convert(final SinkRecord record) {

		Object value = record.value();
		Schema schema = record.valueSchema();

		if (value == null) {
			return Map.of();
		}

		if (value instanceof Map) {
			return (Map<String, Object>) value;
		}

		if (value instanceof Struct) {
			Struct struct = (Struct) value;
			Map<String, Object> result = new HashMap<>();
			schema.fields().forEach(field -> {
				result.put(field.name(), struct.get(field));
			});
			return result;
		}

		if (value instanceof String) {
			try {
				return new ObjectMapper().readValue((String) value, Map.class);
			} catch (Exception e) {
				throw new RuntimeException("Failed to parse JSON string into Map", e);
			}
		}

		if (value instanceof byte[]) {
			try {
				return new ObjectMapper().readValue((byte[]) value, Map.class);
			} catch (Exception e) {
				throw new RuntimeException("Failed to parse JSON bytes into Map", e);
			}
		}

		return new ObjectMapper().convertValue(value, Map.class);

	}

	public Object getSpecificMessageFromKafkaMessage(SinkRecord record, Map<String, Object> value) {
		for (Header header : record.headers()) {
			if ("specificpath".equals(header.key())) {
				String pathKey = header.value().toString();
				Object nested = value.get(pathKey);
				if (nested == null) {
					throw new RuntimeException("Expected " + pathKey + " object, but missing in message");
				}
				return nested;
			}
		}
		return value;
	}
}
