package com.flink.engine.model;

import java.io.Serializable;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

// Use snake_case when this object is serialized to or deserialized from JSON.
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)

// Define the data model for a transaction after enrichment.
public class EnrichedTransaction implements Serializable {
    public String transactionId;            // Store the unique identifier of the transaction.
    public String customerId;               // Store the unique identifier of the customer.
    public String accountId;                // Store the unique identifier of the account.
    public String transactionType;          // Store the type of transaction.
    public double amount;                   // Store the transaction amount.
    public String currency;                 // Store the currency used for the transaction.
    public String merchantId;               // Store the unique identifier of the merchant.
    public String merchantName;             // Store the merchant name.
    public String merchantCategory;         // Store the merchant category.
    public String paymentMethod;            // Store the payment method used for the transaction.
    public String deviceId;                 // Store the identifier of the device used for the transaction.
    public String ipAddress;                // Store the IP address used for the transaction.
    public String city;                     // Store the city where the transaction occurred.
    public String country;                  // Store the country where the transaction occurred.
    public double latitude;                 // Store the latitude of the transaction location.
    public double longitude;                // Store the longitude of the transaction location.
    public String channel;                  // Store the channel used to perform the transaction.
    public int accountAgeDays;              // Store the age of the account in days.
    public String customerRiskLevel;        // Store the customer's risk level.
    public String merchantRiskLevel;        // Store the merchant's risk level.
    public boolean isNewDevice;             // Indicate whether the transaction uses a new device.
    public boolean isNewLocation;           // Indicate whether the transaction occurs at a new location.
    public boolean isForeignTransaction;    // Indicate whether the transaction is performed in a foreign country.
    public boolean isNightTransaction;      // Indicate whether the transaction occurs during nighttime.
    public boolean isHighAmount;            // Indicate whether the transaction amount is considered high.
    public String eventDate;                // Store the transaction event date.
    public int eventHour;                   // Store the hour when the transaction event occurred.
    public String timestamp;                // Store the original transaction timestamp.
    public String processingTime;           // Store the timestamp when the transaction was processed.

    // Provide an empty constructor for object creation and serialization.
    public EnrichedTransaction() {}
}