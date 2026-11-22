package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;

public final class TransactionValidator {
    // Prevent instantiation because this utility class only provides static validation methods.
    private TransactionValidator() {}

    
    // Validate the required fields and business constraints of a RawTransaction.
    public static void validate(RawTransaction transaction) {
        // Reject the transaction when the transaction ID is null or empty.
        if (isBlank(transaction.transactionId)) {
            throw new IllegalArgumentException("transaction_id must not be null");
        }

        // Reject the transaction when the customer ID is null or empty.
        if (isBlank(transaction.customerId)) {
            throw new IllegalArgumentException("customer_id must not be null");
        }

        // Reject the transaction when the transaction amount is zero or negative.
        if (transaction.amount <= 0) {
            throw new IllegalArgumentException("Transaction amount must be greater than zero");
        }

        // Reject the transaction when the event timestamp is zero or negative.
        if (transaction.eventTimestamp <= 0) {
            throw new IllegalArgumentException("timestamp is invalid");
        }
    }


    // Check whether a String is null, empty, or contains only whitespace.
    private static boolean isBlank(String value) {
        // Return true when the value is null or contains no meaningful characters.
        return value == null || value.isBlank();
    }
}