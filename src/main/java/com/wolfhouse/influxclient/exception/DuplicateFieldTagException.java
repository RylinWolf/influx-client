package com.wolfhouse.influxclient.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;

/**
 * @author Rylin Wolf
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class DuplicateFieldTagException extends RuntimeException {
    public static final String DUPLICATE_FIELD_TAG = "重复的标签与字段：";

    public String[] duplicated;

    public DuplicateFieldTagException(String... duplicated) {
        this(DUPLICATE_FIELD_TAG, duplicated);
    }

    public DuplicateFieldTagException(String message, String[] duplicated) {
        super(message + Arrays.toString(duplicated));
        this.duplicated = duplicated;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
