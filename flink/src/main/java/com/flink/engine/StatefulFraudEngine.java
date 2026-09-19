// Ini adalah entry point Job #2

package com.flink.engine;

import com.flink.common.Parameters;
import com.flink.engine.model.EnrichedTransaction;
import com.flink.engine.model.FraudAlert;
import com.flink.engine.model.FraudResult;
import com.flink.engine.model.FraudRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.core.execution.CheckpointingMode;
import java.time.Duration;
import java.time.Instant;

public class StatefulFraudEngine {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
                throw new IllegalArgumentException("Usage: TransactionEnrichment <config-file> [job-name]");
        }
        
        String paramsPath = args[0];
        Parameters params = Parameters.load(paramsPath);
        String jobName = args.length >= 2 ? args[1] : "StatefulFraudEngine";
        String kafkaServer = params.getEnv("KAFKA_SERVER", "kafka:9092");
        String transactionTopic = params.get("kafka.transaction.topic");
        String resultTopic = params.get("kafka.result.topic");
        String alertTopic = params.get("kafka.alert.topic");
        String rulesTopic = params.get("kafka.rules.topic");
        String consumerGroup = params.get("kafka.consumer.group");
        long watermarkDelay = params.getLong("watermark.delay.seconds");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        configureCheckpoint(env, params);
        
        KafkaSource<String> transactionSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServer)
            .setTopics(transactionTopic)
            .setGroupId(consumerGroup)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
            .setBootstrapServers(kafkaServer)
            .setTopics(rulesTopic)
            .setGroupId("fraud-rules")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawTransactions = env
            .fromSource(transactionSource,WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
            .withTimestampAssigner((value, timestamp) -> extractTimestamp(value)),"Transactions Enriched Source");

        DataStream<String> rawRules = env.fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Fraud Rules Source");

        DataStream<EnrichedTransaction> transactions = rawTransactions.map(value -> MAPPER.readValue(value, EnrichedTransaction.class))
            .returns(Types.POJO(EnrichedTransaction.class))
            .name("Parse Enriched Transactions");

        DataStream<FraudRule> rules = rawRules.map(value -> MAPPER.readValue(value,FraudRule.class))
            .returns(Types.POJO(FraudRule.class))
            .name("Parse Fraud Rules");

        MapStateDescriptor<String, FraudRule> ruleStateDescriptor = new MapStateDescriptor <> ("fraud-rules",Types.STRING,Types.POJO(FraudRule.class));
        BroadcastStream<FraudRule> broadcastRules = rules.broadcast(ruleStateDescriptor);
        
        SingleOutputStreamOperator<FraudResult> fraudResults = transactions.keyBy(transaction -> transaction.customerId)
            .connect(broadcastRules)
            .process(new FraudProcessFunction(ruleStateDescriptor))
            .name("Stateful Fraud Engine")
            .uid("stateful-fraud-engine");

        SingleOutputStreamOperator<FraudAlert> fraudAlerts = fraudResults
            .process(new FraudAlertProcessFunction())
            .name("Fraud Alert Generator")
            .uid("fraud-alert-generator");

        fraudResults
            .map(value -> MAPPER.writeValueAsString(value))
            .sinkTo(createKafkaSink(kafkaServer, resultTopic))
            .name("Kafka Fraud Results Sink")
            .uid("kafka-fraud-results-sink");

        fraudAlerts
            .map(value -> MAPPER.writeValueAsString(value))
            .sinkTo(createKafkaSink(kafkaServer, alertTopic))
            .name("Kafka Fraud Alerts Sink")
            .uid("kafka-fraud-alerts-sink");

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

        env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(timeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);

        // Configuration config = new Configuration();
        // config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
        // env.configure(config);

        Configuration parameters = new Configuration();
        parameters.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        parameters.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.path"));
        env.configure(parameters);
    }
    
    private static long extractTimestamp(String json) {
        try {
            EnrichedTransaction transaction = MAPPER.readValue(json, EnrichedTransaction.class);
            return Instant.parse(transaction.timestamp).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
}