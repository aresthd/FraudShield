// Ini adalah entry point Job #2

package com.flink.engine;

import java.time.Duration;
import java.time.Instant;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.common.Parameters;
import com.flink.engine.model.EnrichedTransaction;
import com.flink.engine.model.FraudAlert;
import com.flink.engine.model.FraudResult;
import com.flink.engine.model.FraudRule;

public class StatefulFraudEngine {
    // Create one reusable Jackson ObjectMapper for JSON conversion.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        // Validate that at least the configuration file path was provided.
        if (args.length < 1) {
            throw new IllegalArgumentException("Usage: TransactionEnrichment <config-file> [job-name]");
        }
        
        // Get & Load the configuration file path from the first command-line argument.
        String paramsPath = args[0];
        Parameters params = Parameters.load(paramsPath);

        // Get job name or use default job name StatefulFraudEngine.
        String jobName = args.length >= 2 ? args[1] : "StatefulFraudEngine";
        
		// Get the Kafka configuration
        String kafkaServer = params.getEnv("KAFKA_SERVER", "kafka:9092");
        String transactionTopic = params.get("kafka.transaction.topic");
        String resultTopic = params.get("kafka.result.topic");
        String alertTopic = params.get("kafka.alert.topic");
        String rulesTopic = params.get("kafka.rules.topic");
        String consumerGroup = params.get("kafka.consumer.group");

        // Read the configured watermark delay in seconds.
        long watermarkDelay = params.getLong("watermark.delay.seconds");

        // Create the Flink streaming execution environment.
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Apply checkpoint configuration to the Flink execution environment.
        configureCheckpoint(env, params);
        
        // Create a Kafka source for enriched transactions.
        KafkaSource<String> transactionSource = KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaServer)
                        .setTopics(transactionTopic)
                        .setGroupId(consumerGroup)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();

        // Create a separate Kafka source for fraud rules.
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaServer)
                        .setTopics(rulesTopic)
                        .setGroupId("fraud-rules")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build();
                        
        // Create the transaction stream from the Kafka source with event-time watermarks.
        DataStream<String> rawTransactions = env
            .fromSource(transactionSource,WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))
                .withTimestampAssigner((value, timestamp) -> extractTimestamp(value)),
                "Transactions Enriched Source"
            );

        // Create the fraud rules stream without event-time watermarks.
        DataStream<String> rawRules = env.fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Fraud Rules Source");

        // Convert each transaction JSON string into an EnrichedTransaction object.
        DataStream<EnrichedTransaction> transactions = rawTransactions
            .map(value -> MAPPER.readValue(value, EnrichedTransaction.class))
            .returns(Types.POJO(EnrichedTransaction.class))
            .name("Parse Enriched Transactions");

        // Convert each fraud rule JSON string into a FraudRule object.
        DataStream<FraudRule> rules = rawRules
            .map(value -> MAPPER.readValue(value,FraudRule.class))
            .returns(Types.POJO(FraudRule.class))
            .name("Parse Fraud Rules");

        // Define the Broadcast State structure used to store fraud rules by rule ID.
        MapStateDescriptor<String, FraudRule> ruleStateDescriptor = new MapStateDescriptor<>("fraud-rules", Types.STRING, Types.POJO(FraudRule.class));

        // Broadcast every fraud rule so it is available to all parallel fraud processing tasks.
        BroadcastStream<FraudRule> broadcastRules = rules.broadcast(ruleStateDescriptor);
        
        // Key transactions by customer ID and connect them with the broadcast fraud rules.
        SingleOutputStreamOperator<FraudResult> fraudResults = transactions
            .keyBy(transaction -> transaction.customerId)
            .connect(broadcastRules)
            .process(new FraudProcessFunction(ruleStateDescriptor))
            .name("Stateful Fraud Engine")
            .uid("stateful-fraud-engine");

        // Convert non-normal fraud results into fraud alerts.
        SingleOutputStreamOperator<FraudAlert> fraudAlerts = fraudResults
            .process(new FraudAlertProcessFunction())
            .name("Fraud Alert Generator")
            .uid("fraud-alert-generator");

        // Serialize fraud results into JSON and send them to the result Kafka topic.
        fraudResults
            .map(value -> MAPPER.writeValueAsString(value))
            .sinkTo(createKafkaSink(kafkaServer, resultTopic))
            .name("Kafka Fraud Results Sink")
            .uid("kafka-fraud-results-sink");

        // Serialize fraud alerts into JSON and send them to the alert Kafka topic.
        fraudAlerts
            .map(value -> MAPPER.writeValueAsString(value))
            .sinkTo(createKafkaSink(kafkaServer, alertTopic))
            .name("Kafka Fraud Alerts Sink")
            .uid("kafka-fraud-alerts-sink");

        // Build and start the Flink job with the configured job name.
        env.execute(jobName);
    }

    
    // Create a Kafka sink configured for a specific Kafka topic.
    private static KafkaSink<String> createKafkaSink(String kafkaServer, String topic) {
        // Configure and build the Kafka sink.
        return KafkaSink.<String>builder()
            .setBootstrapServers(kafkaServer)
            .setRecordSerializer(
                    KafkaRecordSerializationSchema.<String>builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
    }
    
    
    // Configure Flink checkpointing based on application parameters.
    private static void configureCheckpoint(StreamExecutionEnvironment env, Parameters params) {
        // Read whether checkpointing is enabled.
        boolean enabled = params.getBoolean("checkpoint.enabled");          

        // Skip checkpoint configuration when checkpointing is disabled.
        if (!enabled) return;

        // Read checkpoint parameters from the configuration
        long interval = params.getLong("checkpoint.interval.ms");
        long timeout = params.getLong("checkpoint.timeout.ms");
        long minPause = params.getLong("checkpoint.min.pause.ms");
        int maxConcurrent = params.getInt("checkpoint.max.concurrent");

        // Set checkpoint configuration in the Flink execution environment
        env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(timeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);

        // Configuration config = new Configuration();
        // config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
        // env.configure(config);

        // Create a Flink configuration object for checkpoint storage settings
        Configuration parameters = new Configuration();
        parameters.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        parameters.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.path"));

        // Apply the checkpoint storage configuration to the Flink environment.
        env.configure(parameters);
    }
    

    // Extract the event timestamp from an enriched transaction JSON message.
    private static long extractTimestamp(String json) {
        // Attempt to parse the transaction timestamp from JSON.
        try {
            // Deserialize the JSON into an EnrichedTransaction object.
            EnrichedTransaction transaction = MAPPER.readValue(json, EnrichedTransaction.class);

            // Convert the transaction timestamp into epoch milliseconds.
            return Instant.parse(transaction.timestamp).toEpochMilli();

        // Handle invalid or missing transaction timestamps.
        } catch (Exception e) {
            // Fall back to the current system time when timestamp extraction fails.
            return System.currentTimeMillis();
        }
    }
}