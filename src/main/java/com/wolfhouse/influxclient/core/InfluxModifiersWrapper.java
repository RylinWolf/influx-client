package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import lombok.Getter;
import lombok.Setter;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

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
    private String                having;
    private InfluxQueryWrapper<T> parent;

    // region 构造方法

    private InfluxModifiersWrapper() {}

    public static <T extends AbstractActionInfluxObj> InfluxModifiersWrapper<T> create(InfluxQueryWrapper<T> parent) {
        InfluxModifiersWrapper<T> wrapper = new InfluxModifiersWrapper<>();
        wrapper.parent = parent;
        return wrapper;
    }

    /**
     * 复制修饰符并绑定新的查询器，独立维护排序和分组集合。
     *
     * @param parent 副本所属的查询器。
     * @return 保留现有修饰符内容的独立副本。
     */
    public InfluxModifiersWrapper<T> copyFor(InfluxQueryWrapper<T> parent) {
        InfluxModifiersWrapper<T> copy = create(parent);
        copy.limit = limit;
        copy.offset = offset;
        copy.globalDesc = globalDesc;
        copy.having = having;
        if (orderBy != null) {
            copy.orderBy = new LinkedHashSet<>(orderBy);
        }
        if (groupBy != null) {
            copy.groupBy = new LinkedHashSet<>(groupBy);
        }
        return copy;
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
        groupBy.addAll(List.of(columns));
        return this;
    }

    /**
     * 添加聚合后过滤条件，允许查询没有 WHERE 条件。
     *
     * @param wrapperConsumer 聚合后过滤条件的构造逻辑。
     * @return 当前修饰符构造器。
     */
    public InfluxModifiersWrapper<T> having(Consumer<InfluxConditionWrapper<T>> wrapperConsumer) {
        // 保留独立的 WHERE 条件容器，仅共享占位符和参数
        InfluxConditionWrapper<T> conditions = this.parent.where();
        // 创建子条件构造器，用于构造 having 条件
        InfluxConditionWrapper<T> wrapper = InfluxConditionWrapper.create(this.parent);
        wrapperConsumer.accept(wrapper);
        // 仅合并参数，聚合别名不参与 WHERE 字段校验
        conditions.getParameters().putAll(wrapper.getParameters());
        String havingCondition = wrapper.sql();
        if (this.having == null) {
            this.having = havingCondition;
        } else {
            this.having += " AND " + havingCondition;
        }
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
        buildHaving(builder);
        buildOrderBy(builder);
        buildLimit(builder);
        return builder.toString().trim();
    }

    protected void buildGroupBy(StringBuilder builder) {
        boolean emptyGroupBy = groupBy == null || groupBy.isEmpty();
        if (emptyGroupBy) {
            return;
        }
        builder.append(" GROUP BY ")
               .append(" ( ");
        groupBy.forEach(c -> builder.append(parent.surroundWithDelimiter(c))
                                    .append(","));
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" ) ");
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

    protected void buildHaving(StringBuilder builder) {
        if (having != null) {
            builder.append(" HAVING (")
                   .append(having)
                   .append(") ");
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
