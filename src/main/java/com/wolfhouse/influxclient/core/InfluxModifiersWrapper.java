package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * InfluxDB 适配的查询修饰符构造器
 * <p>
 * 用于构造查询修饰符，如 order by, limit, offset, group by 等
 *
 * @author Rylin Wolf
 */
@Getter
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class InfluxModifiersWrapper<T extends AbstractActionInfluxObj> {
    private long                  limit  = 0L;
    private long                  offset = 0L;
    private LinkedHashSet<String> orderBy;
    @Setter
    private Boolean               globalDesc;
    private LinkedHashSet<String> groupBy;
    private InfluxQueryWrapper<T> parent;

    // region 构造方法

    private InfluxModifiersWrapper() {}

    public static <T extends AbstractActionInfluxObj> InfluxModifiersWrapper<T> create(InfluxQueryWrapper<T> parent) {
        InfluxModifiersWrapper<T> wrapper = new InfluxModifiersWrapper<>();
        wrapper.parent = parent;
        return wrapper;
    }
    // endregion

    // region 添加修饰符

    /**
     * 设置查询数量
     *
     * @param limit 要查询的数量
     * @return InfluxModifiersWrapper
     */
    public InfluxModifiersWrapper<T> limit(long limit) {
        this.limit = limit;
        return this;
    }

    /**
     * 设置查询偏移量
     *
     * @param offset 偏移量
     * @return InfluxModifiersWrapper
     */
    public InfluxModifiersWrapper<T> offset(long offset) {
        this.offset = offset;
        return this;
    }

    /**
     * 设置查询数量，同时指定偏移量
     *
     * @param limit  要查询的数量
     * @param offset 偏移量
     * @return InfluxModifiersWrapper
     */
    public InfluxModifiersWrapper<T> limit(long limit, long offset) {
        return limit(limit).offset(offset);
    }

    /**
     * 设置排序字段。根据全局排序参数或默认顺序(倒序) 排序。
     *
     * @param columns 需要排序的字段名称，可接收多个字段
     * @return 当前 InfluxModifiersWrapper 实例，便于链式调用
     */
    public InfluxModifiersWrapper<T> orderBy(String... columns) {
        return orderBy(isDesc(), columns);
    }

    /**
     * 设置排序字段。
     *
     * @param desc    是否降序排序，true 表示降序，false 表示升序
     * @param columns 需要排序的字段名称，可接收多个字段
     * @return 当前 InfluxModifiersWrapper 实例，便于链式调用
     */
    public InfluxModifiersWrapper<T> orderBy(boolean desc, String... columns) {
        if (orderBy == null) {
            orderBy = new LinkedHashSet<>();
        }
        for (String column : columns) {
            orderBy.add(column + (desc ? " DESC" : ""));
        }
        return this;
    }

    /**
     * 设置分组字段。
     *
     * @param columns 需要分组的字段名称，可接收多个字段。
     * @return InfluxModifiersWrapper
     */
    public InfluxModifiersWrapper<T> groupBy(String... columns) {
        if (groupBy == null) {
            groupBy = new LinkedHashSet<>();
        }
        groupBy.addAll(Set.of(columns));
        return this;
    }

    // endregion

    // region 执行构建

    public @Nullable String build() {
        return this.parent.build();
    }

    public String toSql() {
        StringBuilder builder = new StringBuilder();
        buildGroupBy(builder);
        buildOrderBy(builder);
        buildLimit(builder);
        return builder.toString().trim();
    }

    protected void buildGroupBy(StringBuilder builder) {
        if (groupBy != null && !groupBy.isEmpty()) {
            builder.append(" GROUP BY ").append(String.join(",", groupBy));
        }
    }

    protected void buildOrderBy(StringBuilder builder) {
        if (orderBy != null && !orderBy.isEmpty()) {
            builder.append(" ORDER BY ").append(String.join(",", orderBy));
        }
    }

    protected void buildLimit(StringBuilder builder) {
        if (limit > 0) {
            builder.append(" LIMIT ").append(limit);
        }
        if (offset > 0) {
            builder.append(" OFFSET ").append(offset);
        }
    }
    // endregion

    // region 私有方法

    /**
     * 获取排序字段是否为降序，默认为 true
     *
     * @return 是否为降序
     */
    private boolean isDesc() {
        return globalDesc == null || globalDesc;
    }
    // endregion
}
