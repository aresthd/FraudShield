package com.flink.engine.model;

import java.io.Serializable;
import java.util.List;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
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
