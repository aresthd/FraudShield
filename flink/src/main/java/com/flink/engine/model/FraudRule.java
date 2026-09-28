package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;


// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model representing a fraud detection rule.
public class FraudRule implements Serializable {
    public String ruleId;               // Store the unique identifier of the fraud rule.
    public String ruleName;             // Store the human-readable name of the fraud rule.
    public String ruleType;             // Store the type or category of the fraud rule.
    public String description;          // Store the description explaining what the rule detects.
    public Double threshold;            // Store the threshold value used by the rule.
    public Integer windowSeconds;       // Store the time window in seconds used by the rule.
    public Integer maxTransactions;     // Store the maximum number of transactions allowed within the configured window.
    public Integer score;               // Store the score assigned when the rule is triggered.
    public String severity;             // Store the severity level assigned to the triggered rule.
    public Boolean enabled;             // Indicate whether the rule is currently enabled.
    public String action;               // Store the action that should be performed when the rule is triggered.
    public Integer version;             // Store the version number of the fraud rule.
    public String updatedAt;            // Store the timestamp when the rule was last updated.
    public String updatedBy;            // Store the identifier of the user or system that updated the rule.
    public String operation;            // Store the operation applied to the rule, such as INSERT, UPDATE, or DELETE.

    // Provide an empty constructor for object creation and serialization.
    public FraudRule() {}
}