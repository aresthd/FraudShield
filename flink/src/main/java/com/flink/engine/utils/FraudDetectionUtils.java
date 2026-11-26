// Utility untuk geographic distance dan impossible travel.
package com.flink.engine.utils;

// Define this class as final so it cannot be inherited.
public final class FraudDetectionUtils {
    // Prevent this utility class from being instantiated.
    private FraudDetectionUtils() {}

    // Calculate the geographical distance between two coordinates in kilometers.
    public static double calculateDistanceKm( double latitude1, double longitude1, double latitude2, double longitude2) {
        // Define the Earth's radius in kilometers for the distance calculation.
        final double earthRadiusKm = 6371.0;

        // Convert latitude and longitude from degrees to radians
        double latDistance = Math.toRadians(latitude2 - latitude1);
        double lonDistance = Math.toRadians(longitude2 - longitude1);

        // Calculate the first part of the Haversine formula using latitude differences.
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
            // Add the longitude component of the Haversine formula.
            + Math.cos(Math.toRadians(latitude1))
            * Math.cos(Math.toRadians(latitude2))
            * Math.sin(lonDistance / 2)
            * Math.sin(lonDistance / 2);

        // Calculate the angular distance between the two geographic points.
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        // Convert the angular distance into kilometers and return the result.
        return earthRadiusKm * c;
    }

    
    // Determine whether the travel speed between two transactions is physically implausible.
    public static boolean isImpossibleTravel(double distanceKm, long elapsedSeconds) {
        // Return false when the elapsed time or distance is invalid for speed calculation.
        if (elapsedSeconds <= 0 || distanceKm <= 0) {
            return false;
        }

        // Convert the elapsed time from seconds to hours.
        double elapsedHours = elapsedSeconds / 3600.0;

        // Calculate the required travel speed in kilometers per hour.
        double speedKmh = distanceKm / elapsedHours;

        // Consider the travel impossible when the required speed exceeds 900 km/h.
        return speedKmh > 900.0;
    }
}