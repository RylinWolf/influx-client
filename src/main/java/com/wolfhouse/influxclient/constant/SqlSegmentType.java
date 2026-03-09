package com.wolfhouse.influxclient.constant;

/**
 * @author Rylin Wolf
 */
public enum SqlSegmentType {
    /** SQL 字段类型 */
    AND("AND"),
    OR("OR"),
    IN("IN", true),
    EQ("="),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">="),
    NE("!="),
    ;

    public final String  value;
    public final boolean isMultiValue;

    SqlSegmentType(String value) {
        this.value        = value;
        this.isMultiValue = false;
    }

    SqlSegmentType(String value, boolean isMultiValue) {
        this.value        = value;
        this.isMultiValue = isMultiValue;
    }


    @Override
    public String toString() {
        return this.value;
    }
}
