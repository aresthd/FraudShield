
// Ini adalah entry point Job #2.

package com.flink.engine;

// Import the project parameter/configuration loader.
import com.flink.common.Parameters;

// Import the enriched transaction data model.
import com.flink.engine.model.EnrichedTransaction;

// Import the fraud alert data model.
import com.flink.engine.model.FraudAlert;

// Import the fraud result data model.
import com.flink.engine.model.FraudResult;

// Import the fraud rule data model.
import com.flink.engine.model.FraudRule;

// Import Jackson for JSON serialization and deserialization.
import com.fasterxml.jackson.databind.ObjectMapper;

// Import Flink watermark strategy for event-time processing.
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

// Import string serializer/deserializer for Kafka messages.
import org.apache.flink.api.common.serialization.SimpleStringSchema;

// Import descriptor used for storing broadcast fraud rules.
import org.apache.flink.api.common.state.MapStateDescriptor;

// Import Flink type information for POJO serialization.
import org.apache.flink.api.common.typeinfo.Types;

// Import Flink checkpointing configuration options.
import org.apache.flink.configuration.CheckpointingOptions;

// Import Flink configuration object.
import org.apache.flink.configuration.Configuration;

// Import Kafka source implementation.
import org.apache.flink.connector.kafka.source.KafkaSource;

// Import Kafka offset initialization strategies.
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

// Import Kafka sink delivery guarantee configuration.
import org.apache.flink.connector.base.DeliveryGuarantee;

// Import Kafka record serialization schema.
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

// Import Kafka sink implementation.
import org.apache.flink.connector.kafka.sink.KafkaSink;

// Import BroadcastStream used to distribute fraud rules.
import org.apache.flink.streaming.api.datastream.BroadcastStream;

// Import DataStream used to represent Flink streams.
import org.apache.flink.streaming.api.datastream.DataStream;

// Import SingleOutputStreamOperator for process transformations with named operators.
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

// Import Flink streaming execution environment.
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Import checkpointing mode configuration.
import org.apache.flink.core.execution.CheckpointingMode;

// Import Duration for watermark configuration.
import java.time.Duration;

// Import Instant for timestamp extraction.
import java.time.Instant;

// Define the entry point for the Stateful Fraud Engine Flink job.
public class StatefulFraudEngine {

    // Create one reusable Jackson ObjectMapper for JSON conversion.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Start the Stateful Fraud Engine Flink job.
    public static void main(String[] args) throws Exception {

        // Validate that at least the configuration file path was provided.
        if (args.length < 1) {

            // Stop startup and show the expected command-line argument format.
            throw new IllegalArgumentException(
                    "Usage: TransactionEnrichment <config-file> [job-name]");
        }

        // Read the configuration file path from the first command-line argument.
        String paramsPath = args[0];

        // Load application configuration from the specified file.
        Parameters params = Parameters.load(paramsPath);

        // Use the provided job name or fall back to StatefulFraudEngine.
        String jobName =
                args.length >= 2
                        ? args[1]
                        : "StatefulFraudEngine";

        // Read the Kafka bootstrap server from configuration or use kafka:9092 as default.
        String kafkaServer =
                params.getEnv("KAFKA_SERVER", "kafka:9092");

        // Read the enriched transaction Kafka topic name.
        String transactionTopic =
                params.get("kafka.transaction.topic");

        // Read the fraud result Kafka topic name.
        String resultTopic =
                params.get("kafka.result.topic");

        // Read the fraud alert Kafka topic name.
        String alertTopic =
                params.get("kafka.alert.topic");

        // Read the fraud rules Kafka topic name.
        String rulesTopic =
                params.get("kafka.rules.topic");

        // Read the Kafka consumer group used by the transaction source.
        String consumerGroup =
                params.get("kafka.consumer.group");

        // Read the configured watermark delay in seconds.
        long watermarkDelay =
                params.getLong("watermark.delay.seconds");

        // Create the Flink streaming execution environment.
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // Apply checkpoint configuration to the Flink execution environment.
        configureCheckpoint(env, params);

        // Create a Kafka source for enriched transactions.
        KafkaSource<String> transactionSource =
                KafkaSource.<String>builder()

                        // Configure the Kafka bootstrap server.
                        .setBootstrapServers(kafkaServer)

                        // Configure the transaction input topic.
                        .setTopics(transactionTopic)

                        // Configure the Kafka consumer group.
                        .setGroupId(consumerGroup)

                        // Start reading from the earliest available Kafka offset.
                        .setStartingOffsets(OffsetsInitializer.earliest())

                        // Deserialize Kafka message values as strings.
                        .setValueOnlyDeserializer(new SimpleStringSchema())

                        // Build the transaction Kafka source.
                        .build();

        // Create a separate Kafka source for fraud rules.
        KafkaSource<String> ruleSource =
                KafkaSource.<String>builder()

                        // Configure the Kafka bootstrap server for the rules source.
                        .setBootstrapServers(kafkaServer)

                        // Configure the fraud rules input topic.
                        .setTopics(rulesTopic)

                        // Use a dedicated consumer group for fraud rules.
                        .setGroupId("fraud-rules")

                        // Start reading fraud rules from the earliest available offset.
                        .setStartingOffsets(OffsetsInitializer.earliest())

                        // Deserialize Kafka rule messages as strings.
                        .setValueOnlyDeserializer(new SimpleStringSchema())

                        // Build the fraud rules Kafka source.
                        .build();

        // Create the transaction stream from the Kafka source with event-time watermarks.
        DataStream<String> rawTransactions =
                env.fromSource(
                        transactionSource,

                        // Allow transactions to arrive out of order within the configured delay.
                        WatermarkStrategy
                                .<String>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(watermarkDelay))

                                // Extract the transaction event timestamp from each JSON message.
                                .withTimestampAssigner(
                                        (value, timestamp) ->
                                                extractTimestamp(value)),

                        // Assign a descriptive name to the transaction source operator.
                        "Transactions Enriched Source");

        // Create the fraud rules stream without event-time watermarks.
        DataStream<String> rawRules =
                env.fromSource(
                        ruleSource,
                        WatermarkStrategy.noWatermarks(),
                        "Fraud Rules Source");

        // Convert each transaction JSON string into an EnrichedTransaction object.
        DataStream<EnrichedTransaction> transactions =
                rawTransactions

                        // Deserialize the Kafka JSON value into an EnrichedTransaction.
                        .map(value ->
                                MAPPER.readValue(
                                        value,
                                        EnrichedTransaction.class))

                        // Tell Flink that the resulting objects are EnrichedTransaction POJOs.
                        .returns(
                                Types.POJO(
                                        EnrichedTransaction.class))

                        // Assign a descriptive name to the parsing operator.
                        .name("Parse Enriched Transactions");

        // Convert each fraud rule JSON string into a FraudRule object.
        DataStream<FraudRule> rules =
                rawRules

                        // Deserialize the Kafka JSON value into a FraudRule object.
                        .map(value ->
                                MAPPER.readValue(
                                        value,
                                        FraudRule.class))

                        // Tell Flink that the resulting objects are FraudRule POJOs.
                        .returns(
                                Types.POJO(
                                        FraudRule.class))

                        // Assign a descriptive name to the rule parsing operator.
                        .name("Parse Fraud Rules");

        // Define the Broadcast State structure used to store fraud rules by rule ID.
        MapStateDescriptor<String, FraudRule> ruleStateDescriptor =
                new MapStateDescriptor<>(
                        "fraud-rules",
                        Types.STRING,
                        Types.POJO(FraudRule.class));

        // Broadcast every fraud rule so it is available to all parallel fraud processing tasks.
        BroadcastStream<FraudRule> broadcastRules =
                rules.broadcast(ruleStateDescriptor);

        // Key transactions by customer ID and connect them with the broadcast fraud rules.
        SingleOutputStreamOperator<FraudResult> fraudResults =
                transactions

                        // Partition transactions so each customer is processed by the same keyed state.
                        .keyBy(transaction ->
                                transaction.customerId)

                        // Connect the keyed transaction stream with the broadcast rule stream.
                        .connect(broadcastRules)

                        // Apply the stateful fraud detection logic.
                        .process(
                                new FraudProcessFunction(
                                        ruleStateDescriptor))

                        // Assign a readable name to the fraud engine operator.
                        .name("Stateful Fraud Engine")

                        // Assign a stable UID for Flink state and savepoint compatibility.
                        .uid("stateful-fraud-engine");

        // Convert non-normal fraud results into fraud alerts.
        SingleOutputStreamOperator<FraudAlert> fraudAlerts =
                fraudResults

                        // Apply the alert generation process function.
                        .process(
                                new FraudAlertProcessFunction())

                        // Assign a readable name to the alert generator operator.
                        .name("Fraud Alert Generator")

                        // Assign a stable UID to the alert generator operator.
                        .uid("fraud-alert-generator");

        // Serialize fraud results into JSON and send them to the result Kafka topic.
        fraudResults

                // Convert each FraudResult object into a JSON string.
                .map(value ->
                        MAPPER.writeValueAsString(value))

                // Send serialized fraud results to Kafka.
                .sinkTo(
                        createKafkaSink(
                                kafkaServer,
                                resultTopic))

                // Assign a readable name to the fraud result sink.
                .name("Kafka Fraud Results Sink")

                // Assign a stable UID to the fraud result sink.
                .uid("kafka-fraud-results-sink");

        // Serialize fraud alerts into JSON and send them to the alert Kafka topic.
        fraudAlerts

                // Convert each FraudAlert object into a JSON string.
                .map(value ->
                        MAPPER.writeValueAsString(value))

                // Send serialized fraud alerts to Kafka.
                .sinkTo(
                        createKafkaSink(
                                kafkaServer,
                                alertTopic))

                // Assign a readable name to the fraud alert sink.
                .name("Kafka Fraud Alerts Sink")

                // Assign a stable UID to the fraud alert sink.
                .uid("kafka-fraud-alerts-sink");

        // Build and start the Flink job with the configured job name.
        env.execute(jobName);
    }

    // Create a Kafka sink configured for a specific Kafka topic.
    private static KafkaSink<String> createKafkaSink(
            String kafkaServer,
            String topic) {

        // Configure and build the Kafka sink.
        return KafkaSink.<String>builder()

                // Configure the Kafka bootstrap server.
                .setBootstrapServers(kafkaServer)

                // Configure how records are serialized before being sent to Kafka.
                .setRecordSerializer(

                        // Create the Kafka record serialization schema.
                        KafkaRecordSerializationSchema
                                .<String>builder()

                                // Set the destination Kafka topic.
                                .setTopic(topic)

                                // Serialize the Kafka value as a string.
                                .setValueSerializationSchema(
                                        new SimpleStringSchema())

                                // Build the record serialization schema.
                                .build())

                // Configure the sink to provide at-least-once delivery semantics.
                .setDeliveryGuarantee(
                        DeliveryGuarantee.AT_LEAST_ONCE)

                // Build the Kafka sink.
                .build();
    }

    // Configure Flink checkpointing based on application parameters.
    private static void configureCheckpoint(
            StreamExecutionEnvironment env,
            Parameters params) {

        // Read whether checkpointing is enabled.
        boolean enabled =
                params.getBoolean("checkpoint.enabled");

        // Skip checkpoint configuration when checkpointing is disabled.
        if (!enabled) return;

        // Read the checkpoint interval from configuration.
        long interval =
                params.getLong("checkpoint.interval.ms");

        // Read the maximum checkpoint execution time.
        long timeout =
                params.getLong("checkpoint.timeout.ms");

        // Read the minimum pause between checkpoints.
        long minPause =
                params.getLong("checkpoint.min.pause.ms");

        // Read the maximum number of concurrent checkpoints.
        int maxConcurrent =
                params.getInt("checkpoint.max.concurrent");

        // Enable periodic checkpoints using exactly-once checkpointing mode.
        env.enableCheckpointing(
                interval,
                CheckpointingMode.EXACTLY_ONCE);

        // Configure the maximum time allowed for a checkpoint to complete.
        env.getCheckpointConfig()
                .setCheckpointTimeout(timeout);

        // Configure the minimum pause between completed checkpoints.
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(minPause);

        // Configure the maximum number of checkpoints allowed to run concurrently.
        env.getCheckpointConfig()
                .setMaxConcurrentCheckpoints(maxConcurrent);

        // Create a Flink configuration object for checkpoint storage settings.
        Configuration parameters =
                new Configuration();

        // Configure the checkpoint storage type as filesystem.
        parameters.set(
                CheckpointingOptions.CHECKPOINT_STORAGE,
                "filesystem");

        // Configure the filesystem path where checkpoints will be stored.
        parameters.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                params.get("checkpoint.path"));

        // Apply the checkpoint storage configuration to the Flink environment.
        env.configure(parameters);
    }

    // Extract the event timestamp from an enriched transaction JSON message.
    private static long extractTimestamp(String json) {

        // Attempt to parse the transaction timestamp from JSON.
        try {

            // Deserialize the JSON into an EnrichedTransaction object.
            EnrichedTransaction transaction =
                    MAPPER.readValue(
                            json,
                            EnrichedTransaction.class);

            // Convert the transaction timestamp into epoch milliseconds.
            return Instant.parse(
                    transaction.timestamp)
                    .toEpochMilli();

        // Handle invalid or missing transaction timestamps.
        } catch (Exception e) {

            // Fall back to the current system time when timestamp extraction fails.
            return System.currentTimeMillis();
        }
    }
}