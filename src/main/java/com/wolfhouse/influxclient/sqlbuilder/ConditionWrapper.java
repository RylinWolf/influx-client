package com.wolfhouse.influxclient.sqlbuilder;

import com.wolfhouse.influxclient.constant.SqlSegmentType;
import com.wolfhouse.influxclient.core.AbstractActionInfluxObj;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * ConditionWrapper 是一个静态内部类，用于组装 SQL 条件查询的构造工具。
 * 该类提供多种方法来构建 AND、OR 逻辑和比较运算符（如 =、<、> 等）条件。
 * 通过动态绑定参数和目标列名称，帮助避免 SQL 注入问题。
 * <p>
 * 注意：
 * 1. 该类的实例不可直接创建，仅支持通过静态方法 `create()` 或 `create(InfluxQueryWrapper<?>)` 创建。
 * 2. 支持拼接复杂的条件查询，并能获取到最终拼接的 SQL 语句片段。
 * <p>
 * 方法特性：
 * - 支持链式调用以构建条件查询。
 * - 提供针对列和参数的安全绑定，避免硬编码风险。
 * - 支持条件流控制，通过布尔值动态决定是否添加条件。
 * <p>
 * 使用场景：
 * - 用于复杂 SQL 查询条件的动态拼接。
 * - 结合 InfluxQueryWrapper 一起使用，生成完整的 SQL 查询语句。
 *
 * @author Rylin Wolf
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class ConditionWrapper<T extends AbstractActionInfluxObj> {
    /** 查询条件参数与值映射 */
    @Getter
    private final Map<String, Object>   parameters;
    /** 查询条件的目标字段 */
    @Getter
    private final Set<String>           targets  = new HashSet<>();
    private final StringBuilder         builder;
    /** 查询条件参数数量 */
    private       AtomicInteger         paramIdx = new AtomicInteger(0);
    /** 父查询链对象 */
    private       InfluxQueryWrapper<T> parent;

    /**
     * 构造一个空的 ConditionWrapper 实例。
     * 初始化参数映射和SQL构建器。
     */
    private ConditionWrapper() {
        parameters = new HashMap<>();
        builder    = new StringBuilder();
    }

    /**
     * 创建一个新的 ConditionWrapper 实例。
     * 该实例不会关联任何父查询对象。
     *
     * @return 返回新创建的 ConditionWrapper 实例
     */
    private static ConditionWrapper<?> create() {
        return new ConditionWrapper<>();
    }

    /**
     * 创建一个新的 ConditionWrapper 实例，并关联指定的父查询对象。
     *
     * @param parent 要关联的父查询对象
     * @return 返回新创建的 ConditionWrapper 实例
     */
    public static <T extends AbstractActionInfluxObj> ConditionWrapper<T> create(InfluxQueryWrapper<T> parent) {
        ConditionWrapper<T> wrapper = new ConditionWrapper<>();
        wrapper.parent = parent;
        return wrapper;
    }

    // region 连接条件

    /**
     * 使用 AND 逻辑连接新的条件。
     * 如果condition为false，则不添加新条件。
     * 若已有条件以 and 结尾，则不再使用 and 连接，直接使用括号构建条件块；
     * 若已有条件为空，则也不使用 and 开头，以避免可能的语法问题。
     *
     * @param consumer  用于构建新条件的消费者函数
     * @param condition 控制是否添加条件的布尔值
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> and(Consumer<ConditionWrapper<T>> consumer, boolean condition) {
        String currentSql = builder.toString().trim().toUpperCase();
        if (currentSql.isBlank() || currentSql.endsWith(SqlSegmentType.AND.value)) {
            builder.append(" ( ")
                   .append(mayDo(condition, consumer, null))
                   .append(" ) ");
            return this;
        }
        builder.append(mayDo(condition, consumer, SqlSegmentType.AND));
        return this;
    }


    /**
     * 使用 AND 逻辑连接新的条件。
     *
     * @param consumer 用于构建新条件的消费者函数
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> and(Consumer<ConditionWrapper<T>> consumer) {
        return and(consumer, true);
    }

    /**
     * 使用 OR 逻辑连接新的条件。
     * 如果condition为false，则不添加新条件。
     *
     * @param consumer  用于构建新条件的消费者函数
     * @param condition 控制是否添加条件的布尔值
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> or(Consumer<ConditionWrapper<T>> consumer, boolean condition) {
        if (builder.toString().trim().toUpperCase().endsWith(SqlSegmentType.OR.value)) {
            builder.append(" ( ")
                   .append(mayDo(condition, consumer, null))
                   .append(" ) ");
        }
        builder.append(mayDo(condition, consumer, SqlSegmentType.OR));
        return this;
    }

    /**
     * 使用 OR 逻辑连接新的条件。
     *
     * @param consumer 用于构建新条件的消费者函数
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> or(Consumer<ConditionWrapper<T>> consumer) {
        return or(consumer, true);
    }
    // endregion

    // region 基本查询条件

    /**
     * 添加等于(=)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> eq(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.EQ) : this;
    }

    public ConditionWrapper<T> eq(String column, Object value) {
        return eq(column, value, true);
    }

    /**
     * 添加不等于(!=)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> ne(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.NE) : this;
    }

    public ConditionWrapper<T> ne(String column, Object value) {
        return ne(column, value, true);
    }

    /**
     * 添加小于(<)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> lt(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.LT) : this;
    }

    public ConditionWrapper<T> lt(String column, Object value) {
        return lt(column, value, true);
    }

    /**
     * 添加大于(>)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> gt(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.GT) : this;
    }

    public ConditionWrapper<T> gt(String column, Object value) {
        return gt(column, value, true);
    }

    /**
     * 添加小于等于(<=)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> le(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.LE) : this;
    }

    public ConditionWrapper<T> le(String column, Object value) {
        return le(column, value, true);
    }

    /**
     * 添加大于等于(>=)条件。
     *
     * @param column    列名
     * @param value     比较值
     * @param condition 是否添加此条件
     * @return 当前 ConditionWrapper 实例
     */
    public ConditionWrapper<T> ge(String column, Object value, boolean condition) {
        return condition ? appendConditionAndMask(column, value, SqlSegmentType.GE) : this;
    }

    public ConditionWrapper<T> ge(String column, Object value) {
        return ge(column, value, true);
    }

    // endregion

    /**
     * 构建完整的SQL查询语句。
     *
     * @return 构建好的SQL查询语句
     */
    public String build() {
        return parent.build();
    }

    /**
     * 获取当前已构建的SQL条件语句。
     *
     * @return 当前条件构造器中的SQL条件语句
     */
    public String sql() {
        return builder.toString();
    }

    /**
     * 获取父查询构造器。
     *
     * @return 返回关联的InfluxQueryWrapper实例
     */
    public InfluxQueryWrapper<T> parent() {
        return parent;
    }

    // region 私有方法

    /**
     * 在已有条件的基础上追加一个带有指定操作符的条件，并根据已存在的条件自动添加 "AND" 连接符。
     *
     * @param column     指定的列名，用于表示查询条件中的字段
     * @param value      列的值，用于进行条件匹配
     * @param sqlSegment 条件操作符，表示具体的 SQL 比较逻辑 (如 "=", "<", ">=" 等)
     * @return 当前 ConditionWrapper 实例，用于支持链式调用
     */
    private ConditionWrapper<T> appendConditionAndMask(String column, Object value, SqlSegmentType sqlSegment) {
        // 若前文有内容，则自动拼接 and 条件，以支持基本条件的链式调用
        boolean and = false;
        if (!this.builder.toString().trim().isEmpty()) {
            and = true;
            this.builder.append(" AND ( ");
        }
        this.builder.append(" ( ").append(column).append(" ")
                    .append(sqlSegment.value)
                    .append(" $").append(addColumnValueMapping(column, value)).append(" ) ");
        // 若拼接 and 条件，则闭合括号
        if (and) {
            this.builder.append(" ) ");
        }
        return this;
    }

    /**
     * 将指定的列名和值映射到参数占位符，并返回生成的参数占位符名称。
     * 此方法主要用于处理 SQL 查询中列名与参数值的绑定，避免硬编码带来的 SQL 注入风险。
     *
     * @param column 指定的列名，用于表示查询条件中的字段
     * @param value  列的值，用于进行条件匹配
     * @return 参数占位符名称，用于在 SQL 查询中代替实际值
     */
    private String addColumnValueMapping(String column, Object value) {
        // 保存目标列名
        this.targets.add(column);
        // 获取值的参数占位名，保存占位符 - 注入值映射，避免 SQL 注入问题
        String valueName = paramName();
        this.parameters.put(valueName, value);
        return valueName;
    }

    /**
     * 根据条件执行SQL片段构建。
     *
     * @param condition  是否执行构建
     * @param consumer   构建逻辑
     * @param sqlSegment SQL片段类型
     * @return 构建的SQL片段
     */
    private String mayDo(boolean condition, Consumer<ConditionWrapper<T>> consumer, SqlSegmentType sqlSegment) {
        if (!condition) {
            return "";
        }
        ConditionWrapper<T> instance = create(this.parent);
        // 同步匿名 wrapper 的上下文（参数数量）
        instance.paramIdx = paramIdx;
        consumer.accept(instance);
        // 获取并添加匿名 wrapper 的处理结果
        this.targets.addAll(instance.targets);
        this.parameters.putAll(instance.parameters);
        if (sqlSegment == null) {
            return instance.sql();
        }
        return " %s (%s)".formatted(sqlSegment.value, instance.sql());
    }

    /**
     * 生成唯一的参数名。
     *
     * @return 生成的参数名
     */
    private String paramName() {
        return "param_" + paramIdx.incrementAndGet();
    }
    // endregion
}