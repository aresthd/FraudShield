package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

public final class TransactionParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TransactionParser() {
    }

    public static RawTransaction parse(String json) {
        RawTransaction transaction = new RawTransaction();
        transaction.rawPayload = json;

        try {
            JsonNode node = MAPPER.readTree(json);

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

            if (timestamp == null || timestamp.isBlank()) {
                throw new IllegalArgumentException("timestamp is required");
            }

            transaction.eventTimestamp = Instant.parse(timestamp).toEpochMilli();
            TransactionValidator.validate(transaction);
            transaction.valid = true;

        } catch (Exception e) {
            transaction.valid = false;
            transaction.errorType = "VALIDATION_ERROR";
            transaction.errorMessage = e.getMessage();
        }

        return transaction;
    }

    private static String text(JsonNode node, String field) {
        if (!node.has(field) || node.get(field).isNull()) {
            return null;
        }
        return node.get(field).asText();
    }
}