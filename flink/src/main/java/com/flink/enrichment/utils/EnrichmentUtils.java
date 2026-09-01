package com.flink.enrichment.utils;

import com.flink.enrichment.model.RawTransaction;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Locale;

public final class EnrichmentUtils {

    private EnrichmentUtils() {
    }

    public static boolean isNewDevice(String previousDevice, String currentDevice) {
        return previousDevice == null || !previousDevice.equals(currentDevice);
    }

    public static boolean isNewLocation(String previousCity, String currentCity) {
        return previousCity == null || !previousCity.equalsIgnoreCase(currentCity);
    }

    public static boolean isNightTransaction(long eventTimestamp) {
        ZonedDateTime dateTime = Instant.ofEpochMilli(eventTimestamp)
                .atZone(ZoneOffset.UTC);

        int hour = dateTime.getHour();

        return hour >= 22 || hour < 6;
    }

    public static boolean isForeignTransaction(String country) {
        return country != null
                && !country.equalsIgnoreCase("Indonesia");
    }

    public static boolean isHighAmount(double amount, double threshold) {
        return amount >= threshold;
    }

    public static String determineMerchantRisk(RawTransaction transaction) {
        String category = transaction.merchantCategory == null
                ? ""
                : transaction.merchantCategory.toUpperCase(Locale.ROOT);

        if (category.contains("GAMBLING")
                || category.contains("CRYPTO")
                || category.contains("HIGH_RISK")) {
            return "HIGH";
        }

        int hash = Math.abs(
                (transaction.merchantId == null
                        ? ""
                        : transaction.merchantId).hashCode()
        );

        if (hash % 10 < 2) {
            return "HIGH";
        }

        if (hash % 10 < 5) {
            return "MEDIUM";
        }

        return "LOW";
    }

    public static int determineAccountAge(String customerId) {
        int hash = Math.abs(customerId.hashCode());
        return 30 + (hash % 1800);
    }

    public static String determineCustomerRisk(int accountAgeDays) {
        if (accountAgeDays < 90) {
            return "HIGH";
        }

        if (accountAgeDays < 365) {
            return "MEDIUM";
        }

        return "LOW";
    }

    public static int eventHour(long eventTimestamp) {
        return Instant.ofEpochMilli(eventTimestamp)
                .atZone(ZoneOffset.UTC)
                .getHour();
    }

    public static String eventDate(long eventTimestamp) {
        return LocalDate.ofInstant(
                Instant.ofEpochMilli(eventTimestamp),
                ZoneOffset.UTC
        ).toString();
    }
}