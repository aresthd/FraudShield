package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;


// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for the fraud detection result.
public class FraudResult implements Serializable {
    // Define the fields to store the fraud detection results information.
    public String resultId;
    public String transactionId;
    public String customerId;
    public double amount;
    public int fraudScore;
    public String status;
    public String riskLevel;
    public List<String> triggeredRules;
    public int ruleCount;
    public int velocityCount;
    public double velocityAmount;
    public String windowStart;
    public String windowEnd;
    public String previousCity;
    public String currentCity;
    public double distanceKm;
    public long timeSinceLastTransactionSec;
    public boolean isImpossibleTravel;
    public String detectionMethod;
    public String processedAt;

    // Provide an empty constructor for object creation and serialization.
    public FraudResult() {}
}