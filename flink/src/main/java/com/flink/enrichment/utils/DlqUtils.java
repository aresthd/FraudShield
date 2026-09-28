package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;

public final class DlqUtils {

    // Create a reusable Jackson ObjectMapper for building DLQ JSON messages.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Read the source Kafka topic from the environment or use the default topic name.
    private static final String SOURCE_TOPIC = System.getenv().getOrDefault("KAFKA_INPUT_TOPIC", "transactions_raw");

    // Prevent instantiation because this utility class only provides static methods.
    private DlqUtils() {}

    // Create a DLQ message containing the invalid transaction and validation error information.
    public static String createDlqMessage(RawTransaction transaction) {
        try {
            // Create an empty JSON object to store the DLQ message.
            ObjectNode dlq = MAPPER.createObjectNode();

            // Write the DLQ data
            dlq.put("dlq_id", "DLQ-" + System.currentTimeMillis());                             // Generate a unique identifier for the DLQ record using the current timestamp.
            dlq.put("source_topic", SOURCE_TOPIC);                                              // Store the Kafka topic where the original transaction was received.
            dlq.put("source_partition", -1);                                                    // Set the source Kafka partition when the partition information is unavailable.
            dlq.put("source_offset", -1L);                                                      // Set the source Kafka offset when the offset information is unavailable.
            dlq.put("event_key",transaction.customerId == null ? "" : transaction.customerId);  // Store the customer ID as the event key or an empty string when the customer ID is unavailable.
            dlq.put("error_type", transaction.errorType);                                       // Store the validation error type generated during transaction processing.
            dlq.put("error_message", transaction.errorMessage == null ? "Unknown validation error" : transaction.errorMessage); // Store the validation error message or use a default message when it is unavailable.
            dlq.put("failed_at_stage", "ENRICHMENT");                                           // Identify the Flink pipeline stage where the transaction failed.
            dlq.put("retry_count", 0);                                                          // Initialize the retry counter because this is the first DLQ record.
            dlq.put("timestamp", Instant.now().toString());                                     // Store the timestamp when the DLQ message is created.

            // Try to preserve the original payload as a structured JSON object.
            try {
                // Parse the original raw payload into a JSON node and attach it to the DLQ message.
                dlq.set("original_payload", MAPPER.readTree(transaction.rawPayload));

            // Handle cases where the original payload is not valid JSON.
            } catch (Exception ignored) {
                // Store the original payload as a plain string when JSON parsing fails.
                dlq.put("original_payload", transaction.rawPayload);
            }

            // Serialize the complete DLQ JSON object into a string for Kafka.
            return MAPPER.writeValueAsString(dlq);

        // Handle unexpected errors that occur while creating the DLQ message.
        } catch (Exception e) {
            // Return a fallback JSON error message when DLQ creation itself fails.
            return "{\"error\":\"Unable to create DLQ message\"}";
        }
    }
}