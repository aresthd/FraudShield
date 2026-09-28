package com.flink.enrichment.model;

public class RawTransaction {
    public String transactionId;        // Store the unique identifier of the transaction.
    public String customerId;           // Store the unique identifier of the customer who made the transaction.
    public String accountId;            // Store the unique identifier of the account used for the transaction.
    public String transactionType;      // Store the type or category of the transaction.
    public double amount;               // Store the monetary value of the transaction.
    public String currency;             // Store the currency used for the transaction amount.
    public String merchantId;           // Store the unique identifier of the merchant involved in the transaction.
    public String merchantName;         // Store the name of the merchant involved in the transaction.
    public String merchantCategory;     // Store the category of the merchant involved in the transaction.
    public String paymentMethod;        // Store the payment method used for the transaction.
    public String deviceId;             // Store the identifier of the device used to perform the transaction.
    public String ipAddress;            // Store the IP address associated with the transaction.
    public String city;                 // Store the city where the transaction occurred.
    public String country;              // Store the country where the transaction occurred.
    public double latitude;             // Store the latitude coordinate of the transaction location.
    public double longitude;            // Store the longitude coordinate of the transaction location.
    public long eventTimestamp;         // Store the event timestamp of the transaction in epoch milliseconds.
    public String channel;              // Store the channel through which the transaction was performed.
    public String rawPayload;           // Store the original raw JSON payload received from Kafka.
    public boolean valid;               // Indicate whether the transaction passed the parsing and validation process.
    public String errorType;            // Store the type of validation or processing error when the transaction is invalid.
    public String errorMessage;         // Store the detailed error message when the transaction is invalid.
}