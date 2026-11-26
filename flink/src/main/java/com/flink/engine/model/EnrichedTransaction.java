package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for a transaction after enrichment.
public class EnrichedTransaction implements Serializable {
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
    public String channel;
    public int accountAgeDays;
    public String customerRiskLevel;
    public String merchantRiskLevel;
    public boolean isNewDevice;
    public boolean isNewLocation;
    public boolean isForeignTransaction;
    public boolean isNightTransaction;
    public boolean isHighAmount;
    public String eventDate;
    public int eventHour;
    public String timestamp;
    public String processingTime;

    // Provide an empty constructor for object creation and serialization.
    public EnrichedTransaction() {}
}