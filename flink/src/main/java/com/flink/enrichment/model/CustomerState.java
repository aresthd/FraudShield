package com.flink.enrichment.model;

public class CustomerState {
    // Define the fields to store the customer's previous transaction information.
    public String lastDeviceId;
    public String lastCity;
    public long lastTransactionTimestamp;
    public double lastAmount;

    // Provide a default constructor required for creating an empty customer state.
    public CustomerState() {}
}