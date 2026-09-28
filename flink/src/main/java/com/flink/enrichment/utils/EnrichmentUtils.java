package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

public final class EnrichmentUtils {
    // Prevent instantiation because this utility class only provides static methods.
    private EnrichmentUtils() {}

    
    // Determine whether the current transaction uses a different device from the previous transaction.
    public static boolean isNewDevice(String previousDevice, String currentDevice) {
        // Return true when there is no previous device or the current device is different.
        return previousDevice == null || !previousDevice.equals(currentDevice);
    }


    // Determine whether the current transaction occurs in a different city from the previous transaction.
    public static boolean isNewLocation(String previousCity, String currentCity) {
        // Return true when there is no previous city or the current city is different regardless of letter case.
        return previousCity == null || !previousCity.equalsIgnoreCase(currentCity);
    }


    // Determine whether a transaction occurred during the defined night-time period.
    public static boolean isNightTransaction(long eventTimestamp) {
        // Convert the event timestamp from milliseconds into a UTC date-time.
        ZonedDateTime dateTime = Instant.ofEpochMilli(eventTimestamp).atZone(ZoneOffset.UTC);

        // Extract the hour from the converted UTC date-time.
        int hour = dateTime.getHour();

        // Return true when the transaction occurs from 22:00 through 05:59 UTC.
        return hour >= 22 || hour < 6;
    }


    // Determine whether a transaction occurred outside Indonesia.
    public static boolean isForeignTransaction(String country) {
        // Return true when the country exists and is not Indonesia.
        return country != null && !country.equalsIgnoreCase("Indonesia");
    }


    // Determine whether the transaction amount reaches or exceeds the configured threshold.
    public static boolean isHighAmount(double amount, double threshold) {
        // Return true when the transaction amount is greater than or equal to the threshold.
        return amount >= threshold;
    }


    // Determine the merchant risk level based on merchant category and a deterministic merchant ID hash.
    public static String determineMerchantRisk(RawTransaction transaction) {
        // Convert the merchant category to uppercase for case-insensitive risk category matching.
        String category = transaction.merchantCategory == null? "" : transaction.merchantCategory.toUpperCase(Locale.ROOT);

        // Mark merchants in explicitly high-risk categories as HIGH risk.
        if (category.contains("GAMBLING") || category.contains("CRYPTO") || category.contains("HIGH_RISK")) {
            return "HIGH";
        }

        // Generate a deterministic hash value from the merchant ID for simulated risk classification.
        int hash = Math.abs((transaction.merchantId == null ? "" : transaction.merchantId).hashCode());

        // Classify approximately 20% of remaining merchants as HIGH risk based on the hash.
        if (hash % 10 < 2) {
            return "HIGH";
        }

        // Classify approximately 30% of remaining merchants as MEDIUM risk based on the hash.
        if (hash % 10 < 5) {
            return "MEDIUM";
        }

        // Classify the remaining merchants as LOW risk.
        return "LOW";
    }


    // Generate a deterministic simulated account age based on the customer ID.
    public static int determineAccountAge(String customerId) {
        // Generate a deterministic hash value from the customer ID.
        int hash = Math.abs(customerId.hashCode());

        // Return an account age between 30 and 1829 days based on the customer hash.
        return 30 + (hash % 1800);
    }


    // Determine the customer risk level based on the customer's account age.
    public static String determineCustomerRisk(int accountAgeDays) {
        // Classify accounts younger than 90 days as HIGH risk.
        if (accountAgeDays < 90) {
            return "HIGH";
        }

        // Classify accounts between 90 and 364 days as MEDIUM risk.
        if (accountAgeDays < 365) {
            return "MEDIUM";
        }

        // Classify accounts that are at least 365 days old as LOW risk.
        return "LOW";
    }


    // Extract the UTC hour from the transaction event timestamp.
    public static int eventHour(long eventTimestamp) {
        // Convert the event timestamp to UTC and return its hour component.
        return Instant.ofEpochMilli(eventTimestamp).atZone(ZoneOffset.UTC).getHour();
    }


    // Extract the UTC calendar date from the transaction event timestamp.
    public static String eventDate(long eventTimestamp) {
        // Convert the event timestamp to a UTC LocalDate and return it as a string.
        return LocalDate.ofInstant(Instant.ofEpochMilli(eventTimestamp), ZoneOffset.UTC).toString();
    }
}