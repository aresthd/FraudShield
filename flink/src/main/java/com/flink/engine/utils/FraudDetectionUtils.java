// Utility untuk geographic distance dan impossible travel.
package com.flink.engine.utils;

public final class FraudDetectionUtils {
    private FraudDetectionUtils() {}

    public static double calculateDistanceKm(double latitude1, double longitude1, double latitude2, double longitude2) {
        final double earthRadiusKm = 6371.0;
        double latDistance = Math.toRadians(latitude2 - latitude1);
        double lonDistance = Math.toRadians(longitude2 - longitude1);

        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(latitude1))
                * Math.cos(Math.toRadians(latitude2))
                * Math.sin(lonDistance / 2)
                * Math.sin(lonDistance / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return earthRadiusKm * c;
    }

    public static boolean isImpossibleTravel(double distanceKm, long elapsedSeconds) {
        if (elapsedSeconds <= 0 || distanceKm <= 0) {
            return false;
        }

        double elapsedHours = elapsedSeconds / 3600.0;
        double speedKmh = distanceKm / elapsedHours;
        return speedKmh > 900.0;
    }
}