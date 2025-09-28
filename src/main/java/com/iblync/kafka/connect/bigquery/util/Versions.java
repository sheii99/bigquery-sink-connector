package com.iblync.kafka.connect.bigquery.util;

public class Versions {

	public static final String NAME = "mongo-kafka";
	public static final String VERSION = "2.0.0"; // from Git
	public static final String BUILD_TIME = "2025-07-06T14:33:05.248Z"; // build timestamp

	public static String getVersion() {
		return VERSION;
	}

	public static String getName() {
		return NAME;
	}

	public static String getBuildTime() {
		return BUILD_TIME;
	}

}