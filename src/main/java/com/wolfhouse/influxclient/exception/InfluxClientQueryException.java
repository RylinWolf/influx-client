package com.wolfhouse.influxclient.exception;

/**
 * @author Rylin Wolf
 */
public class InfluxClientQueryException extends InfluxClientException {
    public InfluxClientQueryException(Throwable cause) {
        super(cause);
    }

    public InfluxClientQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public InfluxClientQueryException() {
    }

    public InfluxClientQueryException(String message) {
        super(message);
    }

    public InfluxClientQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
