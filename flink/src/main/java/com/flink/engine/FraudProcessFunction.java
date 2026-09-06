// Ini merupakan core Stateful Fraud Engine.
// Keyed state menggunakan customer_id, sesuai desain pada summary.md.

/* 
NB:
- estimateDistance() di atas sengaja dibuat sederhana karena transactions_enriched hanya memberikan city pada schema final yang kamu gunakan.
- Kalau ingin impossible travel yang benar-benar akurat, saya lebih menyarankan menyimpan latitude/longitude dari Job #1 ke transactions_enriched.
*/

package com.flink.engine;

import com.flink.engine.model.CustomerBehaviorState;
import com.flink.engine.model.EnrichedTransaction;
import com.flink.engine.model.FraudResult;
import com.flink.engine.model.FraudRule;
import com.flink.engine.utils.FraudDetectionUtils;
import com.flink.engine.utils.FraudScoringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FraudProcessFunction extends KeyedBroadcastProcessFunction<String, EnrichedTransaction, FraudRule, FraudResult> {
    private final MapStateDescriptor<String, FraudRule> ruleStateDescriptor;
    private transient ValueState<CustomerBehaviorState> customerState;
    private transient ListState<TransactionHistory> transactionHistoryState;

    public FraudProcessFunction(MapStateDescriptor<String, FraudRule> ruleStateDescriptor) {
        this.ruleStateDescriptor = ruleStateDescriptor;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<CustomerBehaviorState> customerDescriptor =
                new ValueStateDescriptor<>("customer-behavior-state", Types.POJO(CustomerBehaviorState.class));
        customerState = getRuntimeContext().getState(customerDescriptor);

        ListStateDescriptor<TransactionHistory> historyDescriptor =
                new ListStateDescriptor<>("transaction-history-state", Types.POJO(TransactionHistory.class));
        transactionHistoryState = getRuntimeContext().getListState(historyDescriptor);
    }

    @Override
    public void processBroadcastElement(FraudRule rule, Context ctx, Collector<FraudResult> out) throws Exception {
        if (rule == null || rule.ruleId == null) {
            return;
        }

        BroadcastState<String, FraudRule> state = ctx.getBroadcastState(ruleStateDescriptor);

        if ("DELETE".equalsIgnoreCase(rule.operation)) {
            state.remove(rule.ruleId);
            return;
        }

        FraudRule existing = state.get(rule.ruleId);

        if (existing == null ||
                existing.version == null ||
                rule.version == null ||
                rule.version >= existing.version) {
            state.put(rule.ruleId, rule);
        }
    }

    @Override
    public void processElement(EnrichedTransaction transaction, ReadOnlyContext ctx, Collector<FraudResult> out) throws Exception {
        if (transaction == null || transaction.customerId == null) {
            return;
        }

        long currentTimestamp = transactionTimestamp(transaction);

        CustomerBehaviorState behaviorState = customerState.value();

        if (behaviorState == null) {
            behaviorState = new CustomerBehaviorState();
        }

        List<TransactionHistory> histories = new ArrayList<>();

        for (TransactionHistory history : transactionHistoryState.get()) {
            if (currentTimestamp - history.timestamp <= 30_000L) {
                histories.add(history);
            }
        }

        histories.add(new TransactionHistory(currentTimestamp, transaction.amount));

        int velocityCount = histories.size();
        double velocityAmount = 0.0;

        for (TransactionHistory history : histories) {
            velocityAmount += history.amount;
        }

        long timeSinceLastTransactionSec = 0L;
        double distanceKm = 0.0;
        boolean impossibleTravel = false;

        String previousCity = behaviorState.lastTransactionCity;

        if (behaviorState.lastTransactionTime != null) {
            timeSinceLastTransactionSec =
                    Math.max(0L, (currentTimestamp - behaviorState.lastTransactionTime) / 1000L);

            if (previousCity != null &&
                    transaction.city != null &&
                    !previousCity.equalsIgnoreCase(transaction.city)) {

                distanceKm = estimateDistance(previousCity, transaction.city);

                impossibleTravel =
                        FraudDetectionUtils.isImpossibleTravel(
                                distanceKm,
                                timeSinceLastTransactionSec);
            }
        }

        List<FraudRule> rules = new ArrayList<>();

        for (Map.Entry<String, FraudRule> entry :
                ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {

            FraudRule rule = entry.getValue();

            if (rule != null && Boolean.TRUE.equals(rule.enabled)) {
                rules.add(rule);
            }
        }

        List<String> triggeredRules = new ArrayList<>();

        int fraudScore = 0;

        for (FraudRule rule : rules) {
            if (!matchesRule(
                    rule,
                    transaction,
                    velocityCount,
                    impossibleTravel)) {
                continue;
            }

            if (rule.ruleType != null) {
                triggeredRules.add(rule.ruleType);
            }

            if (rule.score != null) {
                fraudScore += rule.score;
            }
        }

        String status = FraudScoringUtils.classifyStatus(fraudScore);
        String riskLevel = FraudScoringUtils.classifyRiskLevel(fraudScore);

        FraudResult result = new FraudResult();

        result.resultId = "RES-" + UUID.randomUUID();
        result.transactionId = transaction.transactionId;
        result.customerId = transaction.customerId;
        result.amount = transaction.amount;
        result.fraudScore = fraudScore;
        result.status = status;
        result.riskLevel = riskLevel;
        result.triggeredRules = triggeredRules;
        result.ruleCount = triggeredRules.size();
        result.velocityCount = velocityCount;
        result.velocityAmount = velocityAmount;
        result.windowStart =
                Instant.ofEpochMilli(currentTimestamp - 30_000L).toString();
        result.windowEnd =
                Instant.ofEpochMilli(currentTimestamp).toString();
        result.previousCity = previousCity;
        result.currentCity = transaction.city;
        result.distanceKm = distanceKm;
        result.timeSinceLastTransactionSec = timeSinceLastTransactionSec;
        result.isImpossibleTravel = impossibleTravel;
        result.detectionMethod = "RULE_ENGINE";
        result.processedAt = Instant.now().toString();

        transactionHistoryState.update(histories);

        behaviorState.lastTransactionTime = currentTimestamp;
        behaviorState.lastTransactionCity = transaction.city;
        behaviorState.lastDevice = transaction.deviceId;
        behaviorState.lastAmount = transaction.amount;
        behaviorState.transactionCount++;
        behaviorState.totalTransactionAmount += transaction.amount;

        customerState.update(behaviorState);

        out.collect(result);
    }

    private boolean matchesRule(
            FraudRule rule,
            EnrichedTransaction transaction,
            int velocityCount,
            boolean impossibleTravel) {

        if (rule.ruleType == null) {
            return false;
        }

        switch (rule.ruleType) {
            case "HIGH_AMOUNT":
                return rule.threshold != null &&
                        transaction.amount >= rule.threshold;

            case "NEW_DEVICE":
                return transaction.isNewDevice;

            case "NIGHT_TRANSACTION":
                return transaction.isNightTransaction;

            case "VELOCITY":
                return rule.maxTransactions != null &&
                        velocityCount > rule.maxTransactions;

            case "IMPOSSIBLE_TRAVEL":
                return impossibleTravel;

            case "FOREIGN_TRANSACTION":
                return transaction.isForeignTransaction;

            case "NEW_LOCATION":
                return transaction.isNewLocation;

            default:
                return false;
        }
    }

    private long transactionTimestamp(EnrichedTransaction transaction) {
        try {
            return Instant.parse(transaction.timestamp).toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private double estimateDistance(
            String previousCity,
            String currentCity) {

        if (previousCity.equalsIgnoreCase(currentCity)) {
            return 0.0;
        }

        return 1000.0;
    }

    public static class TransactionHistory {
        public long timestamp;
        public double amount;

        public TransactionHistory() {}

        public TransactionHistory(long timestamp, double amount) {
            this.timestamp = timestamp;
            this.amount = amount;
        }
    }
}