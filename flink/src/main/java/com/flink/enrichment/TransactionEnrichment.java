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
        if (args.length < 1) {
                throw new IllegalArgumentException("Usage: TransactionEnrichment <config-file> [job-name]");
        }
        
        String paramsPath = args[0];
        Parameters params = Parameters.load(paramsPath);
        String jobName = args.length >= 2 ? args[1] : "TransactionEnrichment";
        String kafkaServer = params.getEnv("KAFKA_SERVER", "kafka:9092");
		String inputTopic = params.get("kafka.input.topic");
		String outputTopic = params.get("kafka.output.topic");
		String dlqTopic = params.get("kafka.dlq.topic");
		String consumerGroup = params.get("kafka.consumer.group");
		long watermarkDelay = params.getLong("watermark.delay.seconds");
		
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureCheckpoint(env, params);
        
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServer)
                .setTopics(inputTopic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source - transactions_raw"
        );

        DataStream<RawTransaction> parsedStream = rawStream
                .map(TransactionParser::parse)
                .name("Parse Transaction");

        DataStream<RawTransaction> validStream = parsedStream
                .filter(transaction -> transaction.valid)
                .name("Validate Transaction");

        DataStream<String> invalidStream = parsedStream
                .filter(transaction -> !transaction.valid)
                .map(DlqUtils::createDlqMessage)
                .name("Create DLQ");

        DataStream<RawTransaction> eventTimeStream = validStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<RawTransaction>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(watermarkDelay)
                                )
                                .withTimestampAssigner(
                                        (transaction, timestamp) -> transaction.eventTimestamp
                                )
                )
                .name("Event Time & Watermark");

        DataStream<String> enrichedStream = eventTimeStream
                .keyBy(transaction -> transaction.customerId)
                .process(new EnrichmentProcessFunction())
                .name("Customer Enrichment");

        enrichedStream
                .sinkTo(createKafkaSink(kafkaServer, outputTopic))
                .name("Kafka Sink - transactions_enriched");

        invalidStream
                .sinkTo(createKafkaSink(kafkaServer, dlqTopic))
                .name("Kafka Sink - fraud_dlq");

        env.execute(jobName);
    }

	private static KafkaSink<String> createKafkaSink(String kafkaServer, String topic) {
		return KafkaSink.<String>builder()
				.setBootstrapServers(kafkaServer)
				.setRecordSerializer(
						KafkaRecordSerializationSchema.builder()
								.setTopic(topic)
								.setValueSerializationSchema(new SimpleStringSchema())
								.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();
	}

    private static void configureCheckpoint(StreamExecutionEnvironment env, Parameters params) {
		boolean enabled = params.getBoolean("checkpoint.enabled");
        if (!enabled) return;

		long interval = params.getLong("checkpoint.interval.ms");
		long timeout = params.getLong("checkpoint.timeout.ms");
		long minPause = params.getLong("checkpoint.min.pause.ms");
		int maxConcurrent = params.getInt("checkpoint.max.concurrent");
	
        env.enableCheckpointing(interval);
        env.getCheckpointConfig().setCheckpointTimeout(timeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);
        // env.getCheckpointConfig().setCheckpointStorage(
        //         env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints")
        // );

        // env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints"));

    //     Configuration config = new Configuration();
    //     config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    //     config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
	//     env.configure(config);

		Configuration parameters = new Configuration();
		parameters.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
		parameters.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.path"));
		env.configure(parameters);
    }
}