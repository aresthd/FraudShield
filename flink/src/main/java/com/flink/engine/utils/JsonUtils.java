// Karena Kafka menggunakan JSON, utility ini menggunakan Jackson.
package com.flink.engine.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

// Define this class as final so it cannot be inherited.
public final class JsonUtils {
    // Create one reusable ObjectMapper instance for JSON processing.
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Prevent this utility class from being instantiated.
    private JsonUtils() {}

    // Convert a JSON string into an object of the specified Java class.
    public static <T> T fromJson(String json, Class<T> clazz) throws Exception {
        // Deserialize the JSON string into an instance of the requested class.
        return MAPPER.readValue(json, clazz);
    }


    // Convert a Java object into a JSON string.
    public static String toJson(Object object) throws Exception {
        // Serialize the Java object into JSON format.
        return MAPPER.writeValueAsString(object);
    }
}