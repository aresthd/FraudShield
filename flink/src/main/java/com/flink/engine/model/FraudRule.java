package com.flink.engine.model;

import java.io.Serializable;

public class FraudRule implements Serializable {
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

    public FraudRule() {}
}