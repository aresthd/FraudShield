package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;

public final class TransactionValidator {

    private TransactionValidator() {
    }

    public static void validate(RawTransaction transaction) {
        if (isBlank(transaction.transactionId)) {
            throw new IllegalArgumentException("transaction_id must not be null");
        }

        if (isBlank(transaction.customerId)) {
            throw new IllegalArgumentException("customer_id must not be null");
        }

        if (transaction.amount <= 0) {
            throw new IllegalArgumentException("Transaction amount must be greater than zero");
        }

        if (transaction.eventTimestamp <= 0) {
            throw new IllegalArgumentException("timestamp is invalid");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}