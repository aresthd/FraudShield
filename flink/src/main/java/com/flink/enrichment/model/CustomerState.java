package com.flink.enrichment.model;

public class CustomerState {
    public String lastDeviceId;             // Store the device ID used in the customer's previous transaction.
    public String lastCity;                 // Store the city where the customer's previous transaction occurred.
    public long lastTransactionTimestamp;   // Store the event timestamp of the customer's previous transaction.
    public double lastAmount;               // Store the amount of the customer's previous transaction.

    // Provide a default constructor required for creating an empty customer state.
    public CustomerState() {}
}