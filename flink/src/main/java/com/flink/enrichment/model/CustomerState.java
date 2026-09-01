package com.flink.enrichment.model;

public class CustomerState {
    public String lastDeviceId;
    public String lastCity;
    public long lastTransactionTimestamp;
    public double lastAmount;

    public CustomerState() {
    }
}