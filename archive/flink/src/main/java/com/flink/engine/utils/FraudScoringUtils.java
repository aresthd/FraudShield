package com.flink.engine.utils;

// Define this class as final so it cannot be inherited.
public final class FraudScoringUtils {
    // Prevent this utility class from being instantiated.
    private FraudScoringUtils() {}

    /*
        Rule Classification:
        - 0–39  → NORMAL
        - 40–90 → SUSPICIOUS
        - >=90  → FRAUD
    */

    // Convert the total fraud score into a transaction status.
    public static String classifyStatus(int score) {
        // Classify the transaction as FRAUD when the score reaches 90 or higher.
        if (score >= 90) {
            return "FRAUD";
        }

        // Classify the transaction as SUSPICIOUS when the score reaches 40 or higher.
        if (score >= 40) {
            return "SUSPICIOUS";
        }

        // Classify all scores below 40 as NORMAL.
        return "NORMAL";
    }


    // Convert the total fraud score into a risk level.
    public static String classifyRiskLevel(int score) {
        // Assign HIGH risk when the score reaches 90 or higher.
        if (score >= 90) {
            return "HIGH";
        }

        // Assign MEDIUM risk when the score reaches 40 or higher.
        if (score >= 40) {
            return "MEDIUM";
        }

        // Assign LOW risk to all scores below 40.
        return "LOW";
    }


    // Calculate the total fraud score from all individual detection signals.
    public static int calculateScore(int highAmountScore, int newDeviceScore, int nightTransactionScore, int velocityScore, int impossibleTravelScore, int cepScore) {
        // Sum all individual fraud detection scores into one total score.
        return highAmountScore + newDeviceScore + nightTransactionScore + velocityScore + impossibleTravelScore + cepScore;
    }
}