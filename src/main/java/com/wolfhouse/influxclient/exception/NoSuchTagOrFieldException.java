package com.wolfhouse.influxclient.exception;

import java.util.Arrays;

/**
 * @author Rylin Wolf
 */
public class NoSuchTagOrFieldException extends RuntimeException {
    public static final String   NO_SUCH_FIELD_OR_TAG = "标签或字段不存在: ";
    public              String[] names;

    public NoSuchTagOrFieldException(String... names) {
        this(NO_SUCH_FIELD_OR_TAG, names);
    }

    public NoSuchTagOrFieldException(String message, String[] names) {
        super(message + Arrays.toString(names));
        this.names = names;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
