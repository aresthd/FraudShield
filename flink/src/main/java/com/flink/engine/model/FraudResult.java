package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;


// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for the fraud detection result.
public class FraudResult implements Serializable {
    public String resultId;                     // Store the unique identifier of the fraud detection result.
    public String transactionId;                // Store the identifier of the transaction being evaluated.
    public String customerId;                   // Store the identifier of the customer associated with the transaction.
    public double amount;                       // Store the transaction amount.
    public int fraudScore;                      // Store the total fraud score calculated by the fraud engine.
    public String status;                       // Store the status of the fraud detection result.
    public String riskLevel;                    // Store the calculated risk level.
    public List<String> triggeredRules;         // Store the list of fraud rules triggered by the transaction.
    public int ruleCount;                       // Store the number of fraud rules triggered.
    public int velocityCount;                   // Store the number of transactions detected within the configured velocity window.
    public double velocityAmount;               // Store the total transaction amount detected within the velocity window.
    public String windowStart;                  // Store the beginning timestamp of the fraud detection window.
    public String windowEnd;                    // Store the ending timestamp of the fraud detection window.
    public String previousCity;                 // Store the city from the previous transaction.
    public String currentCity;                  // Store the city from the current transaction.
    public double distanceKm;                   // Store the calculated distance between previous and current locations.
    public long timeSinceLastTransactionSec;    // Store the elapsed time since the customer's previous transaction.
    public boolean isImpossibleTravel;          // Indicate whether the transaction represents an impossible travel pattern.
    public String detectionMethod;              // Store the method used to detect the potential fraud.
    public String processedAt;                  // Store the timestamp when the fraud result was processed.

    // Provide an empty constructor for object creation and serialization.
    public FraudResult() {}
}