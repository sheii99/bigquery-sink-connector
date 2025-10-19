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
import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser;
import com.iblync.kafka.connect.bigquery.sink.parser.CDCMessageParser.CDCMessage;
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
	private BigQueryConfig config; // Store config for dynamic handler creation
	private Map<String, String> props; // Store props for handler initialization

	@Override
	public String version() { 
		return Versions.getVersion(); //connector version
	}

	@Override
	public void start(final Map<String, String> props) {
		try {
			this.config = BigquerySinkTopicConfig.parse(props); // instance of the global and topic level config
			this.props = props; // Store props for dynamic handler initialization
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start BigQuerySinkTask due to configuration error", e);
		}
		
		System.out.println("BigQuerySinkTask started with CDC-based dynamic operation selection");
		System.out.println("Configuration loaded successfully - ready to process CDC messages");
	}
	
	/**
		* Creates appropriate operation handler based on CDC operation type
		*/
	private BigQueryHandler createHandlerForOperation(String op) {
		return switch (op.toLowerCase().trim()) {
			case "c", "i" -> Utils.newInstance(InsertOneRow.class); // create/insert
			case "u" -> Utils.newInstance(UpdateOneRow.class);      // update
			case "d" -> Utils.newInstance(DeleteOneRow.class);      // delete
			default -> throw new IllegalArgumentException("Unknown CDC operation: " + op +
				". Expected: c (create), i (insert), u (update), or d (delete)");
		};
	}

	@Override
	public void put(final Collection<SinkRecord> records) { // here ur going to read the records
		if (records.isEmpty()) {
			 System.out.println("put() called with empty records list, skipping.");
			return;
		}
		
		System.out.println("put() called with " + records.size() + " records.");
		
		List<SinkAction> allActions = new ArrayList<>();
		
		for (SinkRecord sinkRecord : records) {
			try {
		         System.out.println("Processing SinkRecord: topic=" + sinkRecord.topic()
		                + ", partition=" + sinkRecord.kafkaPartition()
		                + ", offset=" + sinkRecord.kafkaOffset()
		                + ", key=" + sinkRecord.key()
		                + ", value=" + sinkRecord.value());
		         
		         // Parse CDC message from this record
		         CDCMessage cdcMessage = CDCMessageParser.parse(sinkRecord);
		         System.out.println("Parsed CDC message: " + cdcMessage);
		         
		         // Create handler specific to this record's operation
		         BigQueryHandler recordHandler = createHandlerForOperation(cdcMessage.getOp());
		         recordHandler.init(new SinkHandlerContext(props, config), config);
		         
		         // Process this record with its specific handler
		         SinkHandlerParams params = new SinkHandlerParams(sinkRecord, cdcMessage);
		         SinkAction action = recordHandler.handle(params);
		         allActions.add(action);
		         
			} catch (Exception e) {
				LOGGER.error("Failed to process record from topic={}, partition={}, offset={}",
					sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset(), e);
				// Continue processing other records
			}
		}

		execute(allActions);
	}

	private static void execute(List<SinkAction> actions) {
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
