package com.iblync.kafka.connect.bigquery.sink.handler;

import java.util.ArrayList;
import java.util.List;

import com.iblync.kafka.connect.bigquery.sink.SinkAction;
import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;

public interface BigQueryHandler {

	default void init(SinkHandlerContext context, BigQueryConfig config) {
	}

	SinkAction handle(SinkHandlerParams params); 

	default List<SinkAction> handleBatch(List<SinkHandlerParams> params) {
		List<SinkAction> actions = new ArrayList<>(params.size());
		for (SinkHandlerParams param : params) {
			SinkAction action = handle(param);
			if (action != null) {
				actions.add(action);
			}
		}
		return actions;
	}

}
