// Utility untuk mencari rule dari Broadcast State.
package com.flink.engine.utils;

import com.flink.engine.model.FraudRule;
import org.apache.flink.api.common.state.BroadcastState;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class FraudRuleUtils {
    private FraudRuleUtils() {}
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

    public static List<FraudRule> getEnabledRules(BroadcastState<String, FraudRule> broadcastState) throws Exception {
        List<FraudRule> rules = new ArrayList<>();

        for (Map.Entry<String, FraudRule> entry : broadcastState.immutableEntries()) {
            FraudRule rule = entry.getValue();

            if (rule != null && Boolean.TRUE.equals(rule.enabled)) {
                rules.add(rule);
            }
        }

        return rules;
    }
}