package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;


// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model representing a fraud detection rule.
public class FraudRule implements Serializable {
    // Define the fields to store the fraud detection rule information.
    public String ruleId;
    public String ruleName;
    public String ruleType;
    public String description;
    public Double threshold;
    public Integer windowSeconds;
    public Integer maxTransactions;
    public Integer score;
    public String severity;
    public Boolean enabled;
    public String action;
    public Integer version;
    public String updatedAt;
    public String updatedBy;
    public String operation;

    // Provide an empty constructor for object creation and serialization.
    public FraudRule() {}
}