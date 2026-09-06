// Karena Kafka menggunakan JSON, utility ini menggunakan Jackson.
package com.flink.engine.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private JsonUtils() {}

    public static <T> T fromJson(String json, Class<T> clazz) throws Exception {
        return MAPPER.readValue(json, clazz);
    }

    public static String toJson(Object object) throws Exception {
        return MAPPER.writeValueAsString(object);
    }
}