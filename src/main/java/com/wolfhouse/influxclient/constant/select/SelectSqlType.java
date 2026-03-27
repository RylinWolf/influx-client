package com.wolfhouse.influxclient.constant.select;

/**
 * 查询相关特定函数 SQL 枚举
 *
 * @author Rylin Wolf
 */
public enum SelectSqlType {
    /** max() */
    MAX("max"),
    /** min() */
    MIN("min");
    public final String seg;

    SelectSqlType(String seg) {
        this.seg = seg;
    }
}
