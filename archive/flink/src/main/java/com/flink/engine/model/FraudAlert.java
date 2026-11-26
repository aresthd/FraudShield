package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for a fraud alert generated from a fraud result.
public class FraudAlert implements Serializable {
    public String alertId;                  // Store the unique identifier of the fraud alert.
    public String resultId;                 // Store the identifier of the fraud detection result associated with this alert.
    public String transactionId;            // Store the identifier of the transaction that triggered the alert.
    public String customerId;               // Store the identifier of the customer associated with the alert.
    public String severity;                 // Store the severity level of the alert.
    public int fraudScore;                  // Store the calculated fraud score.
    public String status;                   // Store the current status of the alert.
    public String alertType;                // Store the type of fraud detected.
    public List<String> triggeredRules;     // Store the list of fraud rules triggered by the transaction.
    public String message;                  // Store the human-readable message describing the alert.
    public String createdAt;                // Store the timestamp when the alert was created.

    // Provide an empty constructor for object creation and serialization.
    public FraudAlert() {}
}