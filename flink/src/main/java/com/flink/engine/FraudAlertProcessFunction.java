// Process transformation untuk downstream fraud_alert
package com.flink.engine;

import com.flink.engine.model.FraudAlert;
import com.flink.engine.model.FraudResult;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Instant;
import java.util.UUID;

// Process FraudResult records and generate FraudAlert records.
public class FraudAlertProcessFunction extends ProcessFunction<FraudResult, FraudAlert> {
    // Process one fraud detection result at a time.
    @Override
    public void processElement(FraudResult result, Context ctx, Collector<FraudAlert> out) {
        // Ignore normal transactions because they do not require an alert.
        if ("NORMAL".equals(result.status)) {
            return;
        }

        // Create a new FraudAlert object for the suspicious or fraudulent transaction.
        FraudAlert alert = new FraudAlert();

        // Fill the fraud_alert data
        alert.alertId = "ALT-" + UUID.randomUUID();                             // Generate a unique identifier for the fraud alert.
        alert.resultId = result.resultId;                                       // Link the alert to the original fraud detection result.
        alert.transactionId = result.transactionId;                             // Copy the transaction identifier into the alert.
        alert.customerId = result.customerId;                                   // Copy the customer identifier into the alert.
        alert.severity = "FRAUD".equals(result.status) ? "CRITICAL" : "MEDIUM"; // Assign CRITICAL severity to FRAUD results and MEDIUM severity to other non-normal results.
        alert.fraudScore = result.fraudScore;                                   // Copy the fraud score calculated by the fraud engine.
        alert.status = result.status;                                           // Copy the fraud detection status into the alert.

        // Determine the alert type based on the number of fraud rules triggered.
        alert.alertType = result.triggeredRules.size() > 1
                        // Use MULTI_RULE_FRAUD when multiple rules were triggered.
                        ? "MULTI_RULE_FRAUD"

                        // Use SUSPICIOUS_TRANSACTION when no specific rule is available.
                        : result.triggeredRules.isEmpty()

                        // Use the triggered rule type when exactly one rule was triggered.
                        ? "SUSPICIOUS_TRANSACTION"
                        : result.triggeredRules.get(0);

        alert.triggeredRules = result.triggeredRules;                           // Copy the list of rules that triggered the fraud detection.
        alert.message = "Multiple suspicious transaction patterns detected";    // Set a human-readable message describing the alert.
        alert.createdAt = Instant.now().toString();                             // Record the timestamp when the alert is generated.

        // Emit the generated FraudAlert to the downstream Kafka sink.
        out.collect(alert);
    }
}