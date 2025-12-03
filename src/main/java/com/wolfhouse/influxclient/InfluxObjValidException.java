package com.wolfhouse.influxclient;

/**
 * @author Rylin Wolf
 */
public class InfluxObjValidException extends RuntimeException {
    public static final String VALID_FAILED = "对象验证未通过";

    public InfluxObjValidException(String message) {
        super(message);
    }

    public InfluxObjValidException() {
        super(VALID_FAILED);
    }
}
