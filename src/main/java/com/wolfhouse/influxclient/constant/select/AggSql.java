package com.wolfhouse.influxclient.constant.select;

/**
 * 聚合相关 SQL
 *
 * @author Rylin Wolf
 */
public final class AggSql {
    private AggSql() {
    }

    /**
     * max(col)
     *
     * @param col 列名
     * @return ColSql
     */
    public static ColSql max(String col) {
        return new ColSql(SelectSqlType.MAX, col);
    }

    /**
     * min(col)
     *
     * @param col 列名
     * @return ColSql
     */
    public static ColSql min(String col) {
        return new ColSql(SelectSqlType.MIN, col);
    }

}
