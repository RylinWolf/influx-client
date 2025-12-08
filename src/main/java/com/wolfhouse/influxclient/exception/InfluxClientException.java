package com.wolfhouse.influxclient.exception;

/**
 * @author Rylin Wolf
 */
public class InfluxClientException extends RuntimeException {
    public InfluxClientException() {
    }

    public InfluxClientException(String message) {
        super(message);
    }

    public InfluxClientException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfluxClientException(Throwable cause) {
        super(cause);
    }

    public InfluxClientException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
