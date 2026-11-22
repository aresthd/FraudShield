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
        alert.alertId = "ALT-" + UUID.randomUUID();
        alert.resultId = result.resultId;
        alert.transactionId = result.transactionId;
        alert.customerId = result.customerId;
        alert.severity = "FRAUD".equals(result.status) ? "CRITICAL" : "MEDIUM";
        alert.fraudScore = result.fraudScore;
        alert.status = result.status;

        // Determine the alert type based on the number of fraud rules triggered.
        alert.alertType = result.triggeredRules.size() > 1 ? "MULTI_RULE_FRAUD"
                        : result.triggeredRules.isEmpty() ? "SUSPICIOUS_TRANSACTION"
                        : result.triggeredRules.get(0);

        alert.triggeredRules = result.triggeredRules;
        alert.message = "Multiple suspicious transaction patterns detected";
        alert.createdAt = Instant.now().toString();

        // Emit the generated FraudAlert to the downstream Kafka sink.
        out.collect(alert);
    }
}