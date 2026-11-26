package com.flink.enrichment.model;

public class RawTransaction {
    // Define the fields to store the transaction information.
    public String transactionId;
    public String customerId;
    public String accountId;
    public String transactionType;
    public double amount;
    public String currency;
    public String merchantId;
    public String merchantName;
    public String merchantCategory;
    public String paymentMethod;
    public String deviceId;
    public String ipAddress;
    public String city;
    public String country;
    public double latitude;
    public double longitude;
    public long eventTimestamp;
    public String channel;
    public String rawPayload;
    public boolean valid;
    public String errorType;
    public String errorMessage;
}