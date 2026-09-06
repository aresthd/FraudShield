package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;

public class FraudAlert implements Serializable {
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

    public FraudAlert() {}
}
