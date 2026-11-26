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


// Process keyed transactions together with broadcast fraud rules and produce FraudResult objects.
public class FraudProcessFunction extends KeyedBroadcastProcessFunction<String, EnrichedTransaction, FraudRule, FraudResult> {
    private final MapStateDescriptor<String, FraudRule> ruleStateDescriptor;        // Store the descriptor used to access the fraud rule Broadcast State.
    private transient ValueState<CustomerBehaviorState> customerState;              // Store the latest behavior state for each customer.
    private transient ListState<TransactionHistory> transactionHistoryState;        // Store recent transaction history for each customer.

    
    // Receive and store the Broadcast State descriptor during function construction.
    public FraudProcessFunction(MapStateDescriptor<String, FraudRule> ruleStateDescriptor) {
        // Save the Broadcast State descriptor for later access.
        this.ruleStateDescriptor = ruleStateDescriptor;
    }


    // Initialize Flink managed states when the operator starts.
    @Override
    public void open(OpenContext openContext) throws Exception {
        // Define the descriptor for customer behavior ValueState.
        ValueStateDescriptor<CustomerBehaviorState> customerDescriptor = new ValueStateDescriptor<>( "customer-behavior-state", Types.POJO(CustomerBehaviorState.class));

        // Register the customer behavior state with Flink's runtime state backend.
        customerState = getRuntimeContext().getState(customerDescriptor);

        // Define the descriptor for recent transaction history ListState.
        ListStateDescriptor<TransactionHistory> historyDescriptor = new ListStateDescriptor<>( "transaction-history-state", Types.POJO(TransactionHistory.class));

        // Register the transaction history state with Flink's runtime state backend.
        transactionHistoryState = getRuntimeContext().getListState(historyDescriptor);
    }


    // Process each incoming fraud rule and update the Broadcast State.
    @Override
    public void processBroadcastElement(FraudRule rule, Context ctx, Collector<FraudResult> out) throws Exception {
        // Ignore invalid rules that do not contain a rule identifier.
        if (rule == null || rule.ruleId == null) {
            return;
        }

        // Get the writable Broadcast State associated with the rule descriptor.
        BroadcastState<String, FraudRule> state = ctx.getBroadcastState(ruleStateDescriptor);

        // Remove the rule from Broadcast State when the operation is DELETE.
        if ("DELETE".equalsIgnoreCase(rule.operation)) {
            // Delete the rule using its rule identifier as the key.
            state.remove(rule.ruleId);

            // Stop processing this rule after deletion.
            return;
        }

        // Retrieve the currently stored version of this rule.
        FraudRule existing = state.get(rule.ruleId);

        // Update the state only when the incoming rule is newer or no existing rule exists.
        if (existing == null || existing.version == null || rule.version == null || rule.version >= existing.version) {
            // Store or replace the fraud rule in Broadcast State.
            state.put(rule.ruleId, rule);
        }
    }


    // Process each enriched transaction and evaluate it against the active fraud rules.
    @Override
    public void processElement(EnrichedTransaction transaction, ReadOnlyContext ctx, Collector<FraudResult> out) throws Exception {

        // Ignore transactions that do not contain a customer identifier.
        if (transaction == null || transaction.customerId == null) {
            return;
        }

        // Extract the event timestamp from the transaction.
        long currentTimestamp = transactionTimestamp(transaction);

        // Retrieve the previously stored behavior state for this customer.
        CustomerBehaviorState behaviorState = customerState.value();

        // Create a new behavior state when this is the customer's first transaction.
        if (behaviorState == null) {
            behaviorState = new CustomerBehaviorState();
        }

        // Create a temporary list for transaction history within the velocity window.
        List<TransactionHistory> histories = new ArrayList<>();

        // Iterate through the customer's previously stored transaction history.
        for (TransactionHistory history : transactionHistoryState.get()) {
            // Keep only transactions that occurred within the previous 30 seconds.
            if (currentTimestamp - history.timestamp <= 30_000L) {
                // Add the recent transaction to the active velocity window.
                histories.add(history);
            }
        }

        // Add the current transaction to the velocity window.
        histories.add(new TransactionHistory(currentTimestamp, transaction.amount));

        // Count the number of transactions inside the velocity window.
        int velocityCount = histories.size();

        // Initialize data
        double velocityAmount = 0.0;
        long timeSinceLastTransactionSec = 0L;
        double distanceKm = 0.0;
        boolean impossibleTravel = false;
        int fraudScore = 0;

        // Create array list for rules
        List<FraudRule> rules = new ArrayList<>();
        List<String> triggeredRules = new ArrayList<>();

        
        // Iterate through all transactions in the velocity window.
        for (TransactionHistory history : histories) {
            // Add each transaction amount to the velocity total.
            velocityAmount += history.amount;
        }
 
        // Retrieve the city from the customer's previous transaction.
        String previousCity = behaviorState.lastTransactionCity;

        // Continue travel analysis only when a previous transaction timestamp exists.
        if (behaviorState.lastTransactionTime != null) {
            // Calculate the elapsed time between the current and previous transaction.
            timeSinceLastTransactionSec = Math.max(0L, (currentTimestamp - behaviorState.lastTransactionTime) / 1000L);

            // Check whether the current and previous transactions occurred in different cities.
            if (previousCity != null && transaction.city != null && !previousCity.equalsIgnoreCase(transaction.city)) {
                // Estimate the distance between the previous and current cities.
                distanceKm = estimateDistance(previousCity, transaction.city);
                // Determine whether the required travel speed is impossible.
                impossibleTravel = FraudDetectionUtils.isImpossibleTravel(distanceKm, timeSinceLastTransactionSec);
            }
        }

        // Iterate through all fraud rules stored in the Broadcast State.
        for (Map.Entry<String, FraudRule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            // Retrieve the FraudRule object from the current Broadcast State entry.
            FraudRule rule = entry.getValue();

            // Check that the rule exists and is explicitly enabled.
            if (rule != null && Boolean.TRUE.equals(rule.enabled)) {
                // Add the enabled rule to the list used for transaction evaluation.
                rules.add(rule);
            }
        }

        // Evaluate the current transaction against every enabled fraud rule.
        for (FraudRule rule : rules) {
            // Skip the rule when the current transaction does not match its conditions.
            if (!matchesRule(rule, transaction, velocityCount, impossibleTravel)) {
                continue;
            }

            // Record the rule type when the rule is triggered.
            if (rule.ruleType != null) {
                triggeredRules.add(rule.ruleType);
            }

            // Add the rule's configured score to the total fraud score.
            if (rule.score != null) {
                fraudScore += rule.score;
            }
        }

        // Convert total fraud score into classification labels
        String status = FraudScoringUtils.classifyStatus(fraudScore);
        String riskLevel = FraudScoringUtils.classifyRiskLevel(fraudScore);

        // Create a new FraudResult object containing the detection result.
        FraudResult result = new FraudResult();

        // Fill the fraud_result data
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
        result.windowStart = Instant.ofEpochMilli(currentTimestamp - 30_000L).toString();
        result.windowEnd = Instant.ofEpochMilli(currentTimestamp).toString();
        result.previousCity = previousCity;
        result.currentCity = transaction.city;
        result.distanceKm = distanceKm;
        result.timeSinceLastTransactionSec = timeSinceLastTransactionSec;
        result.isImpossibleTravel = impossibleTravel;
        result.detectionMethod = "RULE_ENGINE";
        result.processedAt = Instant.now().toString();

        // Persist the current transaction history for future velocity checks.
        transactionHistoryState.update(histories);

        // Update the behavior state
        behaviorState.lastTransactionTime = currentTimestamp;
        behaviorState.lastTransactionCity = transaction.city;
        behaviorState.lastDevice = transaction.deviceId;
        behaviorState.lastAmount = transaction.amount;
        behaviorState.transactionCount++;
        behaviorState.totalTransactionAmount += transaction.amount;

        // Persist the updated customer behavior state in Flink.
        customerState.update(behaviorState);

        // Emit the completed fraud result to downstream operators.
        out.collect(result);
    }


    // Determine whether a transaction matches the conditions of a specific fraud rule.
    private boolean matchesRule(FraudRule rule, EnrichedTransaction transaction, int velocityCount, boolean impossibleTravel) {
        // Reject the rule when no rule type is defined.
        if (rule.ruleType == null) {
            return false;
        }

        // Select the fraud detection logic based on the rule type.
        switch (rule.ruleType) {
            // Check whether the transaction amount exceeds the configured threshold.
            case "HIGH_AMOUNT":
                return rule.threshold != null && transaction.amount >= rule.threshold;

            // Check whether the transaction uses a newly detected device.
            case "NEW_DEVICE":
                return transaction.isNewDevice;

            // Check whether the transaction occurred during nighttime.
            case "NIGHT_TRANSACTION":
                return transaction.isNightTransaction;

            // Check whether the number of recent transactions exceeds the configured limit.
            case "VELOCITY":
                return rule.maxTransactions != null && velocityCount > rule.maxTransactions;

            // Check whether the customer's travel between transactions is impossible.
            case "IMPOSSIBLE_TRAVEL":
                return impossibleTravel;

            // Check whether the transaction occurred in a foreign country.
            case "FOREIGN_TRANSACTION":
                return transaction.isForeignTransaction;

            // Check whether the transaction occurred at a new location.
            case "NEW_LOCATION":
                return transaction.isNewLocation;

            // Reject unknown rule types by default.
            default:
                return false;
        }
    }


    // Extract the transaction timestamp as epoch milliseconds.
    private long transactionTimestamp(EnrichedTransaction transaction) {
        // Attempt to parse the transaction timestamp.
        try {
            // Convert the ISO-8601 timestamp into epoch milliseconds.
            return Instant.parse(transaction.timestamp).toEpochMilli();

        // Handle invalid or missing timestamp values.
        } catch (Exception e) {
            // Fall back to the current system time when parsing fails.
            return System.currentTimeMillis();
        }
    }


    // Estimate the distance between two transaction cities.
    private double estimateDistance(String previousCity, String currentCity) {
        // Return zero when both transactions occurred in the same city.
        if (previousCity.equalsIgnoreCase(currentCity)) {
            return 0.0;
        }

        // Use a fixed 1000 km estimate when the cities are different.
        return 1000.0;
    }


    // Define the data structure used to store one transaction in the history state.
    public static class TransactionHistory {
        // Store the transaction data
        public long timestamp;
        public double amount;

        // Provide an empty constructor required by Flink's POJO serialization.
        public TransactionHistory() {}

        // Create a transaction history entry with timestamp and amount.
        public TransactionHistory(long timestamp, double amount) {
            // Store the transaction data
            this.timestamp = timestamp;
            this.amount = amount;
        }
    }
}