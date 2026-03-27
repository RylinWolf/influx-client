package com.wolfhouse.influxclient.constant.select;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Influx 查询 SQL，参数为列名类型
 *
 * @author Rylin Wolf
 */
@Getter
@Accessors(chain = true, fluent = true)
public class ColSql {
    /** SQL 类型 */
    private final SelectSqlType      type;
    /** 列名 */
    private final Collection<String> cols;
    /** 别名 */
    private       String             alias;

    protected ColSql(SelectSqlType type, String... cols) {
        this.type = type;
        this.cols = new ArrayList<>(Arrays.asList(cols));
    }

    protected ColSql(SelectSqlType type, Collection<String> cols) {
        this.type = type;
        this.cols = new ArrayList<>(cols);
    }

    /**
     * 设置列的别名
     *
     * @param alias 别名
     * @return 当前 ColSql 实例，支持链式调用
     */
    public ColSql as(String alias) {
        this.alias = alias;
        return this;
    }
}
