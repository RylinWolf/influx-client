package com.wolfhouse.influxclient.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * @author Rylin Wolf
 */
public class ServiceUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}
