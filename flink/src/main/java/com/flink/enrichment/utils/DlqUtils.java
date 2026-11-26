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
            dlq.put("dlq_id", "DLQ-" + System.currentTimeMillis());
            dlq.put("source_topic", SOURCE_TOPIC);
            dlq.put("source_partition", -1);
            dlq.put("source_offset", -1L);
            dlq.put("event_key",transaction.customerId == null ? "" : transaction.customerId);
            dlq.put("error_type", transaction.errorType);
            dlq.put("error_message", transaction.errorMessage == null ? "Unknown validation error" : transaction.errorMessage);
            dlq.put("failed_at_stage", "ENRICHMENT");
            dlq.put("retry_count", 0);
            dlq.put("timestamp", Instant.now().toString());

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