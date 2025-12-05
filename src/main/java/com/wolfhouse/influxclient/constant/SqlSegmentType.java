package com.wolfhouse.influxclient.constant;

/**
 * @author Rylin Wolf
 */
public enum SqlSegmentType {
    /** SQL 字段类型 */
    AND("AND"),
    OR("OR"),
    EQ("="),
    LT("<"),
    GT(">"),
    LE("<="),
    GE(">="),
    ;

    public final String value;

    SqlSegmentType(String value) {
        this.value = value;
    }


    @Override
    public String toString() {
        return this.value;
    }
}
