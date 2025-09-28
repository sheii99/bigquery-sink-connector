package com.iblync.kafka.connect.bigquery.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicMap { // return map of strings ||convert the strings into topic based. 
	// then get the values from the topics 

	public static Map<String, String> parseTopicToValues(List<String> topicToValues, @Nullable String defaultValues) {
		return parseCommon(topicToValues);
	}

	private static Map<String, String> parseCommon(List<String> map) {
		Map<String, String> result = new HashMap<>();
		for (String entry : map) {
			String[] components = entry.split("=", -1);
			if (components.length != 2) {
				throw new IllegalArgumentException("Bad entry: '" + entry
						+ "'. Expected exactly one equals (=) character separating key and value.");
			}
			result.put(components[0], components[1]);
		}
		return result;
	}

	/*
	 * private static <K, V1, V2> Map<K, V2> mapValues(Map<K, V1> map, Function<?
	 * super V1, ? extends V2> valueTransformer) { return map.entrySet().stream()
	 * .collect(toMap(Map.Entry::getKey, entry ->
	 * valueTransformer.apply(entry.getValue()))); }
	 */
}
