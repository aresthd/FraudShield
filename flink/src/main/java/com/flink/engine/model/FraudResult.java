package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;

public class FraudResult implements Serializable {
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

    public FraudResult() {}
}