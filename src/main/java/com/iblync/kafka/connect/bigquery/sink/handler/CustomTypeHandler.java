package com.iblync.kafka.connect.bigquery.sink.handler;


import org.apache.kafka.common.config.ConfigDef;

public interface CustomTypeHandler<T> {
    T valueOf(String value);

    default ConfigDef.Validator validator() {
        return null;
    }

    default ConfigDef.Recommender recommender() {
        return null;
    }
}
