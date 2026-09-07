// MAIN FILE

package com.flink.enrichment;

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

    private static final String KAFKA_SERVER = env("KAFKA_SERVER", "kafka:9092");
    private static final String INPUT_TOPIC = env("KAFKA_TOPIC_RAW", "transactions_raw");
    private static final String OUTPUT_TOPIC = env("KAFKA_TOPIC_ENRICHED", "transactions_enriched");
    private static final String DLQ_TOPIC = env("KAFKA_TOPIC_DLQ", "fraud_dlq");
    private static final String CONSUMER_GROUP = env("KAFKA_CONSUMER_GROUP_ENRICHMENT", "fraud-enrichment");
    private static final long WATERMARK_DELAY_SECONDS = Long.parseLong(env("WATERMARK_DELAY_SECONDS", "5"));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureCheckpoint(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP)
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
                                        Duration.ofSeconds(WATERMARK_DELAY_SECONDS)
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
                .sinkTo(createKafkaSink(OUTPUT_TOPIC))
                .name("Kafka Sink - transactions_enriched");

        invalidStream
                .sinkTo(createKafkaSink(DLQ_TOPIC))
                .name("Kafka Sink - fraud_dlq");

        env.execute("TransactionEnrichment");
    }

    private static KafkaSink<String> createKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        boolean enabled = Boolean.parseBoolean(env("CHECKPOINT_ENABLED", "true"));
        if (!enabled) return;

        long interval = Long.parseLong(env("CHECKPOINT_INTERVAL", "30000"));
        long timeout = Long.parseLong(env("CHECKPOINT_TIMEOUT", "60000"));
        long minPause = Long.parseLong(env("CHECKPOINT_MIN_PAUSE", "5000"));
        int maxConcurrent = Integer.parseInt(env("CHECKPOINT_MAX_CONCURRENT", "1"));

        env.enableCheckpointing(interval);
        env.getCheckpointConfig().setCheckpointTimeout(timeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);
        // env.getCheckpointConfig().setCheckpointStorage(
        //         env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints")
        // );

        // env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // env.getCheckpointConfig().set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH", "file:///opt/flink/checkpoints"));

        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
        env.configure(config);
    }


    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}