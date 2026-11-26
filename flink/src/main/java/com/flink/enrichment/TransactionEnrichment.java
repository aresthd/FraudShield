// MAIN FILE

package com.flink.enrichment;

import com.flink.common.Parameters;
import com.flink.enrichment.utils.*;
import com.flink.enrichment.model.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public class TransactionEnrichment {

	public static void main(String[] args) throws Exception {
		// Validate that the configuration file path is provided as a command-line argument.
		if (args.length < 1) {
			throw new IllegalArgumentException("Usage: TransactionEnrichment <config-file> [job-name]");
		}

		// Get & Load the configuration file path from the first command-line argument.
		String paramsPath = args[0];
		Parameters params = Parameters.load(paramsPath);

		// Get the Flink job name from the second argument or use the default job name.
		String jobName = args.length >= 2 ? args[1] : "TransactionEnrichment";

		// Get the Kafka configuration
		String kafkaServer = params.getEnv("KAFKA_SERVER", "kafka:9092");
		String inputTopic = params.get("kafka.input.topic");
		String outputTopic = params.get("kafka.output.topic");
		String dlqTopic = params.get("kafka.dlq.topic");
		String consumerGroup = params.get("kafka.consumer.group");

		// Get the allowed event-time out-of-order delay used for watermark generation.
		long watermarkDelay = params.getLong("watermark.delay.seconds");

		// Create the Flink execution environment used to build and execute the streaming pipeline.
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Configure Flink checkpointing based on the application configuration.
		configureCheckpoint(env, params);

		// Build a Kafka source that reads raw transaction messages from the configured Kafka topic.
		KafkaSource<String> source = KafkaSource.<String>builder()
			.setBootstrapServers(kafkaServer)
			.setTopics(inputTopic)
			.setGroupId(consumerGroup)
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			.build();

		// Create a Flink DataStream from the Kafka source without generating watermarks at this stage.
		DataStream<String> rawStream = env.fromSource(
			source, WatermarkStrategy.noWatermarks(),
			"Kafka Source - transactions_raw"
		);

		// Parse raw JSON/string messages into RawTransaction objects.
		DataStream<RawTransaction> parsedStream = rawStream
			.map(TransactionParser::parse)
			.name("Parse Transaction");

		// Keep only transactions that successfully passed the validation process.
		DataStream<RawTransaction> validStream = parsedStream
			.filter(transaction -> transaction.valid)
			.name("Validate Transaction");

		// Keep invalid transactions and convert them into messages for the dead-letter queue.
		DataStream<String> invalidStream = parsedStream
			.filter(transaction -> !transaction.valid)
			.map(DlqUtils::createDlqMessage)
			.name("Create DLQ");

		// Assign event timestamps and generate watermarks to handle out-of-order transactions.
		DataStream<RawTransaction> eventTimeStream = validStream
			.assignTimestampsAndWatermarks(
				WatermarkStrategy.<RawTransaction>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
				.withTimestampAssigner((transaction, timestamp) -> transaction.eventTimestamp)
			)
			.name("Event Time & Watermark");

		// Group transactions by customer so that enrichment state can be maintained per customer.
		DataStream<String> enrichedStream = eventTimeStream
			.keyBy(transaction -> transaction.customerId)
			.process(new EnrichmentProcessFunction())
			.name("Customer Enrichment");

		// Send successfully enriched transactions to the configured Kafka output topic.
		enrichedStream.sinkTo(createKafkaSink(kafkaServer, outputTopic))
			.name("Kafka Sink - transactions_enriched");

		// Send invalid transactions to the configured DLQ Kafka topic.
		invalidStream.sinkTo(createKafkaSink(kafkaServer, dlqTopic))
			.name("Kafka Sink - fraud_dlq");

		// Submit and execute the complete Flink streaming job.
		env.execute(jobName);
	}

	
	// Create a Kafka sink that publishes string messages to the specified Kafka topic.
	private static KafkaSink<String> createKafkaSink(String kafkaServer, String topic) {
		// Build and configure the Kafka sink.
		return KafkaSink.<String>builder()
			// Configure the Kafka broker address used by the sink.
			.setBootstrapServers(kafkaServer)

			// Configure how records are serialized and which topic receives them.
			.setRecordSerializer(
				KafkaRecordSerializationSchema.builder()
				.setTopic(topic)
				.setValueSerializationSchema(new SimpleStringSchema())
				.build()
			)

			// Guarantee that each record is delivered at least once.
			.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)

			// Build the Kafka sink.
			.build();
	}

	// Configure Flink checkpointing using values from the application configuration.
	private static void configureCheckpoint(StreamExecutionEnvironment env, Parameters params) {
		// Read whether checkpointing is enabled from the configuration.
		boolean enabled = params.getBoolean("checkpoint.enabled");

		// Stop checkpoint configuration when checkpointing is disabled.
		if (!enabled) return;

		// Get checkpoint configurations
		long interval = params.getLong("checkpoint.interval.ms");
		long timeout = params.getLong("checkpoint.timeout.ms");
		long minPause = params.getLong("checkpoint.min.pause.ms");
		int maxConcurrent = params.getInt("checkpoint.max.concurrent");
	
		// Enable checkpointing with the configured interval.
        env.enableCheckpointing(interval);

        // Configure the maximum time allowed for each checkpoint.
        env.getCheckpointConfig().setCheckpointTimeout(timeout);

        // Configure the minimum pause between checkpoint executions.
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);

        // Configure the maximum number of concurrent checkpoints.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);
		
		// env.getCheckpointConfig().setCheckpointStorage(
		//         env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints")
		// );

		// env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
		// env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints"));

		// Configuration config = new Configuration();
		// config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
		// config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
		// env.configure(config);

		// Configure the checkpoint storage type and location.
        Configuration parameters = new Configuration();

        // Store checkpoint state using the filesystem backend.
        parameters.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");

        // Set the checkpoint directory from the application configuration.
        parameters.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.path"));

        // Apply the checkpoint storage configuration to the Flink environment.
        env.configure(parameters);
	}
}