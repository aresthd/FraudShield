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
                        .setBootstrapServers(kafkaServer)                       // Configure the Kafka bootstrap server.
                        .setTopics(transactionTopic)                            // Configure the transaction input topic.
                        .setGroupId(consumerGroup)                              // Configure the Kafka consumer group.
                        .setStartingOffsets(OffsetsInitializer.earliest())      // Start reading from the earliest available Kafka offset.
                        .setValueOnlyDeserializer(new SimpleStringSchema())     // Deserialize Kafka message values as strings.
                        .build();                                               // Build the transaction Kafka source.

        // Create a separate Kafka source for fraud rules.
        KafkaSource<String> ruleSource = KafkaSource.<String>builder()
                        .setBootstrapServers(kafkaServer)                       // Configure the Kafka bootstrap server for the rules source.
                        .setTopics(rulesTopic)                                  // Configure the fraud rules input topic.
                        .setGroupId("fraud-rules")                              // Use a dedicated consumer group for fraud rules.
                        .setStartingOffsets(OffsetsInitializer.earliest())      // Start reading fraud rules from the earliest available offset.
                        .setValueOnlyDeserializer(new SimpleStringSchema())     // Deserialize Kafka rule messages as strings.
                        .build();                                               // Build the fraud rules Kafka source.
                        
        // Create the transaction stream from the Kafka source with event-time watermarks.
        DataStream<String> rawTransactions = env
            .fromSource(transactionSource,WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelay))    // Allow transactions to arrive out of order within the configured delay.
            .withTimestampAssigner((value, timestamp) -> extractTimestamp(value)),                                                  // Extract the transaction event timestamp from each JSON message.
                "Transactions Enriched Source");                                                                                    // Assign a descriptive name to the transaction source operator.

        // Create the fraud rules stream without event-time watermarks.
        DataStream<String> rawRules = env.fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Fraud Rules Source");

        // Convert each transaction JSON string into an EnrichedTransaction object.
        DataStream<EnrichedTransaction> transactions = rawTransactions
            .map(value -> MAPPER.readValue(value, EnrichedTransaction.class))   // Deserialize the Kafka JSON value into an EnrichedTransaction.
            .returns(Types.POJO(EnrichedTransaction.class))                     // Tell Flink that the resulting objects are EnrichedTransaction POJOs.
            .name("Parse Enriched Transactions");                               // Assign a descriptive name to the parsing operator.

        // Convert each fraud rule JSON string into a FraudRule object.
        DataStream<FraudRule> rules = rawRules
            .map(value -> MAPPER.readValue(value,FraudRule.class))      // Deserialize the Kafka JSON value into a FraudRule object.
            .returns(Types.POJO(FraudRule.class))                       // Tell Flink that the resulting objects are FraudRule POJOs.
            .name("Parse Fraud Rules");                                 // Assign a descriptive name to the rule parsing operator.

        // Define the Broadcast State structure used to store fraud rules by rule ID.
        MapStateDescriptor<String, FraudRule> ruleStateDescriptor = new MapStateDescriptor<>("fraud-rules", Types.STRING, Types.POJO(FraudRule.class));

        // Broadcast every fraud rule so it is available to all parallel fraud processing tasks.
        BroadcastStream<FraudRule> broadcastRules = rules.broadcast(ruleStateDescriptor);
        
        // Key transactions by customer ID and connect them with the broadcast fraud rules.
        SingleOutputStreamOperator<FraudResult> fraudResults = transactions
            .keyBy(transaction -> transaction.customerId)               // Partition transactions so each customer is processed by the same keyed state.
            .connect(broadcastRules)                                    // Connect the keyed transaction stream with the broadcast rule stream.
            .process(new FraudProcessFunction(ruleStateDescriptor))     // Apply the stateful fraud detection logic.
            .name("Stateful Fraud Engine")                              // Assign a readable name to the fraud engine operator.
            .uid("stateful-fraud-engine");                              // Assign a stable UID for Flink state and savepoint compatibility.

        // Convert non-normal fraud results into fraud alerts.
        SingleOutputStreamOperator<FraudAlert> fraudAlerts = fraudResults
            .process(new FraudAlertProcessFunction())   // Apply the alert generation process function.
            .name("Fraud Alert Generator")              // Assign a readable name to the alert generator operator.
            .uid("fraud-alert-generator");              // Assign a stable UID to the alert generator operator.

        // Serialize fraud results into JSON and send them to the result Kafka topic.
        fraudResults
            .map(value -> MAPPER.writeValueAsString(value))         // Convert each FraudResult object into a JSON string.
            .sinkTo(createKafkaSink(kafkaServer, resultTopic))      // Send serialized fraud results to Kafka.
            .name("Kafka Fraud Results Sink")                       // Assign a readable name to the fraud result sink.
            .uid("kafka-fraud-results-sink");                       // Assign a stable UID to the fraud result sink.

        // Serialize fraud alerts into JSON and send them to the alert Kafka topic.
        fraudAlerts
            .map(value -> MAPPER.writeValueAsString(value))         // Convert each FraudAlert object into a JSON string.
            .sinkTo(createKafkaSink(kafkaServer, alertTopic))       // Send serialized fraud alerts to Kafka.
            .name("Kafka Fraud Alerts Sink")                        // Assign a readable name to the fraud alert sink.
            .uid("kafka-fraud-alerts-sink");                        // Assign a stable UID to the fraud alert sink.

        // Build and start the Flink job with the configured job name.
        env.execute(jobName);
    }

    
    // Create a Kafka sink configured for a specific Kafka topic.
    private static KafkaSink<String> createKafkaSink(String kafkaServer, String topic) {
        // Configure and build the Kafka sink.
        return KafkaSink.<String>builder()
            .setBootstrapServers(kafkaServer)                                           // Configure the Kafka bootstrap server.
            .setRecordSerializer(                                                       // Configure how records are serialized before being sent to Kafka.
                    KafkaRecordSerializationSchema.<String>builder()                    // Create the Kafka record serialization schema.
                            .setTopic(topic)                                            // Set the destination Kafka topic.
                            .setValueSerializationSchema(new SimpleStringSchema())      // Serialize the Kafka value as a string.
                            .build())                                                   // Build the record serialization schema.
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)                      // Configure the sink to provide at-least-once delivery semantics.
            .build();                                                                   // Build the Kafka sink.
    }
    
    
    // Configure Flink checkpointing based on application parameters.
    private static void configureCheckpoint(StreamExecutionEnvironment env, Parameters params) {
        boolean enabled = params.getBoolean("checkpoint.enabled");          // Read whether checkpointing is enabled.

        // Skip checkpoint configuration when checkpointing is disabled.
        if (!enabled) return;

        long interval = params.getLong("checkpoint.interval.ms");           // Read the checkpoint interval from configuration.
        long timeout = params.getLong("checkpoint.timeout.ms");             // Read the maximum checkpoint execution time.
        long minPause = params.getLong("checkpoint.min.pause.ms");          // Read the minimum pause between checkpoints.
        int maxConcurrent = params.getInt("checkpoint.max.concurrent");     // Read the maximum number of concurrent checkpoints.

        env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE);      // Enable periodic checkpoints using exactly-once checkpointing mode.
        env.getCheckpointConfig().setCheckpointTimeout(timeout);                // Configure the maximum time allowed for a checkpoint to complete.
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPause);      // Configure the minimum pause between completed checkpoints.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);   // Configure the maximum number of checkpoints allowed to run concurrently.

        // Configuration config = new Configuration();
        // config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        // config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, env("CHECKPOINT_PATH_ENRICHMENT", "file:///opt/flink/checkpoints"));
        // env.configure(config);

        // Create a Flink configuration object for checkpoint storage settings.
        Configuration parameters = new Configuration();
        parameters.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");                               // Configure the checkpoint storage type as filesystem.
        parameters.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, params.get("checkpoint.path"));      // Configure the filesystem path where checkpoints will be stored.

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