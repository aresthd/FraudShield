package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

public final class TransactionParser {
    // Create a reusable Jackson ObjectMapper for parsing JSON transaction messages.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Prevent instantiation because this utility class only provides static methods.
    private TransactionParser() {}

    // Parse a raw Kafka JSON message into a RawTransaction object and validate its contents.
    public static RawTransaction parse(String json) {
        // Create an empty RawTransaction object to store the parsed transaction data.
        RawTransaction transaction = new RawTransaction();

        // Preserve the original Kafka message for debugging and DLQ purposes.
        transaction.rawPayload = json;
        try {
            // Parse the raw JSON string into a Jackson JsonNode tree.
            JsonNode node = MAPPER.readTree(json);

            // Extract raw transaction data
            transaction.transactionId = text(node, "transaction_id");
            transaction.customerId = text(node, "customer_id");
            transaction.accountId = text(node, "account_id");
            transaction.transactionType = text(node, "transaction_type");
            transaction.amount = node.has("amount") ? node.get("amount").asDouble() : 0.0;
            transaction.currency = text(node, "currency");
            transaction.merchantId = text(node, "merchant_id");
            transaction.merchantName = text(node, "merchant_name");
            transaction.merchantCategory = text(node, "merchant_category");
            transaction.paymentMethod = text(node, "payment_method");
            transaction.deviceId = text(node, "device_id");
            transaction.ipAddress = text(node, "ip_address");
            transaction.city = text(node, "city");
            transaction.country = text(node, "country");
            transaction.latitude = node.has("latitude") ? node.get("latitude").asDouble() : 0.0;
            transaction.longitude = node.has("longitude") ? node.get("longitude").asDouble() : 0.0;
            transaction.channel = text(node, "channel");
            String timestamp = text(node, "timestamp");

            // Reject the transaction when the timestamp is missing or empty.
            if (timestamp == null || timestamp.isBlank()) {
                throw new IllegalArgumentException("timestamp is required");
            }

            // Convert the ISO-8601 timestamp into milliseconds since the Unix epoch.
            transaction.eventTimestamp = Instant.parse(timestamp).toEpochMilli();

            // Validate the parsed transaction against the transaction validation rules.
            TransactionValidator.validate(transaction);

            // Mark the transaction as valid after parsing and validation succeed.
            transaction.valid = true;

        // Handle JSON parsing, timestamp, or validation errors.
        } catch (Exception e) {
            // Mark the transaction as invalid when any parsing or validation error occurs.
            transaction.valid = false;

            // Store a generic error category for the invalid transaction.
            transaction.errorType = "VALIDATION_ERROR";

            // Store the actual exception message for debugging and DLQ processing.
            transaction.errorMessage = e.getMessage();
        }

        // Return the parsed transaction together with its validation status and error information.
        return transaction;
    }

    
    // Safely extract a text value from a JSON field.
    private static String text(JsonNode node, String field) {
        // Return null when the requested field does not exist or contains a JSON null value.
        if (!node.has(field) || node.get(field).isNull()) {
            return null;
        }

        // Convert the JSON field value into a Java String.
        return node.get(field).asText();
    }
}