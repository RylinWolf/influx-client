package com.wolfhouse.influxclient.exception;

/**
 * @author Rylin Wolf
 */
public class InfluxClientInsertException extends InfluxClientException {
    public InfluxClientInsertException() {
    }

    public InfluxClientInsertException(String message) {
        super(message);
    }

    public InfluxClientInsertException(String message, Throwable cause) {
        super(message, cause);
    }

    public InfluxClientInsertException(Throwable cause) {
        super(cause);
    }

    public InfluxClientInsertException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
