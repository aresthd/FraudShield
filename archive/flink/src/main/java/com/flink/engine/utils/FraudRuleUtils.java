// Utility untuk mencari rule dari Broadcast State.
package com.flink.engine.utils;

import com.flink.engine.model.FraudRule;
import org.apache.flink.api.common.state.BroadcastState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Define this class as final so it cannot be inherited.
public final class FraudRuleUtils {
    // Prevent this utility class from being instantiated.
    private FraudRuleUtils() {}

    // Define the name used to identify the fraud rule broadcast state.
    public static final String RULE_STATE_NAME = "fraud-rules";

    // public static List<FraudRule> getEnabledRules(BroadcastState<String, FraudRule> broadcastState) throws Exception {
    //     List<FraudRule> rules = new ArrayList<>();

    //     for (FraudRule rule : broadcastState.immutableEntries()) {
    //         FraudRule value = rule;
    //     }

    //     for (java.util.Map.Entry<String, FraudRule> entry : broadcastState.immutableEntries()) {
    //         FraudRule rule = entry.getValue();

    //         if (rule != null && Boolean.TRUE.equals(rule.enabled)) {
    //             rules.add(rule);
    //         }
    //     }

    //     return rules;

    // }


    // Return all enabled fraud rules stored in the Broadcast State.
    public static List<FraudRule> getEnabledRules(BroadcastState<String, FraudRule> broadcastState) throws Exception {
        // Create a list to store fraud rules that are currently enabled.
        List<FraudRule> rules = new ArrayList<>();

        // Iterate through all entries currently stored in the Broadcast State.
        for (Map.Entry<String, FraudRule> entry : broadcastState.immutableEntries()) {
            // Get the FraudRule object from the current Broadcast State entry.
            FraudRule rule = entry.getValue();

            // Check that the rule exists and is explicitly marked as enabled.
            if (rule != null && Boolean.TRUE.equals(rule.enabled)) {
                // Add the enabled fraud rule to the result list.
                rules.add(rule);
            }
        }

        // Return all enabled fraud rules to the caller.
        return rules;
    }
}