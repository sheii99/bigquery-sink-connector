package com.iblync.kafka.connect.bigquery.sink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.iblync.kafka.connect.bigquery.sink.config.BigQueryConfig;
import com.iblync.kafka.connect.bigquery.sink.handler.BigQueryHandler;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerContext;
import com.iblync.kafka.connect.bigquery.sink.handler.SinkHandlerParams;
import com.iblync.kafka.connect.bigquery.sink.operations.DeleteOneRow;
import com.iblync.kafka.connect.bigquery.sink.operations.InsertOneRow;
import com.iblync.kafka.connect.bigquery.sink.operations.UpdateOneRow;
import com.iblync.kafka.connect.bigquery.util.BatchBuilder;
import com.iblync.kafka.connect.bigquery.util.Versions;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

//receives records from Kafka topics and write them to BigQuery. 
public class BigQuerySinkTask extends SinkTask { 
	// sinkTask is a kafka connect class that writes data from Kafka to external systems.

	static final Logger LOGGER = LoggerFactory.getLogger(BigQuerySinkTask.class);
	private static final String CONNECTOR_TYPE = "sink";
	private static final String BOOSTRAP_SERVER = "bootstrap.server";
	private static final String prefix = "bigquery."; //CONNECTOR_TYPE, BOOSTRAP_SERVER, prefix: Configuration constants.
	private BigQueryHandler sinkHandler; // Object that handles the logic of inserting/updating/deleting BigQuery rows.

	@Override
	public String version() { 
		return Versions.getVersion(); //connector version
	}

	@Override
	public void start(final Map<String, String> props) {
		BigQueryConfig config;
		try {
			config = BigquerySinkTopicConfig.parse(props); // instance of the global and topic level config 
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start CouchbaseSinkTask due to configuration error", e);
		}

		sinkHandler = switch (config.operations()) {
		case "UPDATE" -> Utils.newInstance(UpdateOneRow.class);
		case "INSERT" -> Utils.newInstance(InsertOneRow.class);
		case "DELETE" -> Utils.newInstance(DeleteOneRow.class);
		default -> throw new IllegalArgumentException("Unknown operation: " + config.operations());
		};
		
		sinkHandler.init(new SinkHandlerContext(props,config),config); // initialized the handler with the configuration context. 

	}

	@Override
	public void put(final Collection<SinkRecord> records) { // here ur going to read the records 
		if (records.isEmpty()) {
			return;
		}

		List<SinkHandlerParams> paramsList = new ArrayList<>();
		for (SinkRecord sinkRecord : records) {
			try {
				SinkHandlerParams params = new SinkHandlerParams(sinkRecord); // it considered as a 
				paramsList.add(params);
			} catch (Exception e) {
				LOGGER.warn("{}", e);
			}
		}
		List<SinkAction> actions = sinkHandler.handleBatch(paramsList); // actions 

		execute(actions);
	}

	private static void execute(List<SinkAction> actions) {// sysprintln 
		Duration timeout = Duration.ofMinutes(10);

		toMono(actions).block(timeout);
	}

	static Mono<Void> toMono(List<SinkAction> actions) {
		BatchBuilder<Mono<Void>> batchBuilder = new BatchBuilder<>();
		for (SinkAction action : actions) {
			batchBuilder.add(action.action(), action.concurrencyHint());
		}

		Stream<Mono<Void>> batches = batchBuilder.build().stream()
				.map(batch -> Flux.fromIterable(batch).flatMap(it -> it).then());

		return Flux.fromStream(batches).concatMap(it -> it).then();
	}

	@Override
	public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
		LOGGER.debug("Flush called - noop");
	}

	@Override
	public void stop() {

	}
}
