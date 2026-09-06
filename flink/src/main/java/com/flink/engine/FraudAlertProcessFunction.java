// Karena fraud_results dan fraud_alerts merupakan dua output berbeda, saya sarankan membuat alert sebagai downstream transformation.

package com.flink.engine;

import com.flink.engine.model.FraudAlert;
import com.flink.engine.model.FraudResult;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Instant;
import java.util.UUID;

public class FraudAlertProcessFunction extends ProcessFunction<FraudResult, FraudAlert> {

    @Override
    public void processElement(FraudResult result, Context ctx, Collector<FraudAlert> out) {

        if ("NORMAL".equals(result.status)) {
            return;
        }

        FraudAlert alert = new FraudAlert();

        alert.alertId = "ALT-" + UUID.randomUUID();
        alert.resultId = result.resultId;
        alert.transactionId = result.transactionId;
        alert.customerId = result.customerId;
        alert.severity =
                "FRAUD".equals(result.status)
                        ? "CRITICAL"
                        : "MEDIUM";
        alert.fraudScore = result.fraudScore;
        alert.status = result.status;
        alert.alertType =
                result.triggeredRules.size() > 1
                        ? "MULTI_RULE_FRAUD"
                        : result.triggeredRules.isEmpty()
                        ? "SUSPICIOUS_TRANSACTION"
                        : result.triggeredRules.get(0);

        alert.triggeredRules = result.triggeredRules;
        alert.message = "Multiple suspicious transaction patterns detected";

        alert.createdAt = Instant.now().toString();

        out.collect(alert);
    }
}