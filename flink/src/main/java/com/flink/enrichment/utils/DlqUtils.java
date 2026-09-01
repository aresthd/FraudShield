package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;

public final class DlqUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SOURCE_TOPIC = System.getenv().getOrDefault(
            "KAFKA_INPUT_TOPIC",
            "transactions_raw"
    );

    private DlqUtils() {
    }

    public static String createDlqMessage(RawTransaction transaction) {
        try {
            ObjectNode dlq = MAPPER.createObjectNode();

            dlq.put("dlq_id", "DLQ-" + System.currentTimeMillis());
            dlq.put("source_topic", SOURCE_TOPIC);
            dlq.put("source_partition", -1);
            dlq.put("source_offset", -1L);
            dlq.put(
                    "event_key",
                    transaction.customerId == null ? "" : transaction.customerId
            );
            dlq.put("error_type", transaction.errorType);
            dlq.put(
                    "error_message",
                    transaction.errorMessage == null
                            ? "Unknown validation error"
                            : transaction.errorMessage
            );
            dlq.put("failed_at_stage", "ENRICHMENT");
            dlq.put("retry_count", 0);
            dlq.put("timestamp", Instant.now().toString());

            try {
                dlq.set(
                        "original_payload",
                        MAPPER.readTree(transaction.rawPayload)
                );
            } catch (Exception ignored) {
                dlq.put("original_payload", transaction.rawPayload);
            }

            return MAPPER.writeValueAsString(dlq);

        } catch (Exception e) {
            return "{\"error\":\"Unable to create DLQ message\"}";
        }
    }
}