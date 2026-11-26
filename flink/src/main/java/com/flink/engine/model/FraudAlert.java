package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for a fraud alert generated from a fraud result.
public class FraudAlert implements Serializable {
    // Define the fields to store the fraud alert information.
    public String alertId;
    public String resultId;
    public String transactionId;
    public String customerId;
    public String severity;
    public int fraudScore;
    public String status;
    public String alertType;
    public List<String> triggeredRules;
    public String message;
    public String createdAt;

    // Provide an empty constructor for object creation and serialization.
    public FraudAlert() {}
}