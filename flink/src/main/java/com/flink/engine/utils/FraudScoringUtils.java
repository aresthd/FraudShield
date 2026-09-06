package com.flink.engine.utils;

public final class FraudScoringUtils {
    private FraudScoringUtils() {}

    /* 
        Rule Classification:
        - 0–39  → NORMAL
        - 40–90 → SUSPICIOUS
        - >=90  → FRAUD
    */
    public static String classifyStatus(int score) {
        if (score >= 90) {
            return "FRAUD";
        }
        if (score >= 40) {
            return "SUSPICIOUS";
        }
        return "NORMAL";
    }

    public static String classifyRiskLevel(int score) {
        if (score >= 90) {
            return "HIGH";
        }
        if (score >= 40) {
            return "MEDIUM";
        }
        return "LOW";
    }

    public static int calculateScore(int highAmountScore, int newDeviceScore, int nightTransactionScore, int velocityScore, int impossibleTravelScore, int cepScore) {
        return highAmountScore + newDeviceScore + nightTransactionScore + velocityScore + impossibleTravelScore + cepScore;
    }
}