package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.InfluxClientConstant;
import com.wolfhouse.influxclient.exception.NoSuchTagOrFieldException;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 基于 {@link AbstractInfluxObj} 的查询构建器
 *
 * @author Rylin Wolf
 */
@Slf4j
@Data
@Accessors(chain = true)
public class InfluxQueryWrapper<T extends AbstractInfluxObj> {
    /** 是否为匿名构造器，即未通过泛型创建的构造器 */
    private boolean               isLambda      = false;
    /** 查询结果中包括时间戳 */
    private boolean               withTime      = true;
    /** 目标表名 */
    private String                measurement;
    /** 映射对象引用 */
    private AbstractInfluxObj     reference;
    /** 映射对象包含的标签约束 */
    private InfluxTags            tags;
    /** 映射对象包含的字段约束 */
    private InfluxFields          fields;
    /** 查询目标列 */
    private LinkedHashSet<String> queryTargets  = new LinkedHashSet<>();
    /** 查询条件构造器 */
    private ConditionWrapper      conditionWrapper;
    /** 是否已经构建过 */
    private boolean               isBuild       = false;
    /** 是否拥有查询条件，该值仅在构建后才有效 */
    private boolean               isConditioned = false;

    // region 构造方法

    /**
     * 构造一个带有引用对象的 InfluxQueryWrapper 实例，并初始化引用对象。
     *
     * @param reference 被包装的引用对象，用于提供查询或构建 SQL 的相关信息。
     */
    private InfluxQueryWrapper(T reference) {
        this.reference = reference;
        initReference();
    }

    /**
     * 构造一个 InfluxQueryWrapper 实例，并设置测量名称。
     *
     * @param measurement 测量名称，用于指定目标数据表。
     */
    private InfluxQueryWrapper(String measurement) {
        measurement(measurement);
        initReference();
    }

    private InfluxQueryWrapper() {}

    // endregion

    // region 获取实例

    /**
     * 从指定的 {@link AbstractInfluxObj} 对象创建并初始化一个 {@link InfluxQueryWrapper} 实例。
     *
     * @param obj 提供初始化数据的对象，该对象必须继承自 {@link AbstractInfluxObj}。
     *            通过调用该对象的 {@code getMeasurement()} 方法获得测量名称。
     * @return 初始化好的 {@link InfluxQueryWrapper} 实例，其中包含传入对象的测量名称。
     */
    public static <T extends AbstractInfluxObj> InfluxQueryWrapper<T> from(T obj) {
        return new InfluxQueryWrapper<>(obj);
    }

    public static <T extends AbstractInfluxObj> String fromBuild(T obj) {
        InfluxQueryWrapper<T> wrapper = new InfluxQueryWrapper<>(obj);
        wrapper.selectSelfTag();
        wrapper.selectSelfField();
        return wrapper.build();
    }

    /**
     * 创建一个新的 {@link InfluxQueryWrapper} 实例，未初始化任何测量名称或引用对象。
     *
     * @return 一个新的 {@link InfluxQueryWrapper} 实例，测量名称和引用对象均为空。
     */
    public static InfluxQueryWrapper<?> create() {
        InfluxQueryWrapper<?> wrapper = new InfluxQueryWrapper<>();
        wrapper.isLambda = true;
        return wrapper;
    }

    /**
     * 创建一个带有指定测量名称的 InfluxQueryWrapper 实例。
     *
     * @param measurement 测量名称，用于指定数据查询的目标表。
     * @return 返回一个初始化了测量名称的 InfluxQueryWrapper 实例。
     */
    public static InfluxQueryWrapper<?> create(String measurement) {
        return new InfluxQueryWrapper<>(measurement);
    }
    // endregion

    // region 设置方法

    public InfluxQueryWrapper<T> measurement(String measurement) {
        assert measurement != null && !measurement.isBlank() : "表名 (measurement) 不得为空！";
        this.measurement = measurement;
        // 设置表名后，若初始化对象不存在，则初始化引用对象
        if (reference == null) {
            initReference();
        }
        return this;
    }
    // endregion

    // region 构建查询

    /**
     * 设置查询的目标字段列表。
     * 该方法会将其添加到内部的查询目标字段队列中。
     *
     * @param fields 查询的字段列表，表示需要在返回结果中包含的字段名称。
     *               字段列表中的字段名必须存在于标签集合或字段集合中，否则会触发验证异常。
     * @return 当前 InfluxQueryWrapper 实例，用于支持链式调用。
     * @throws NoSuchTagOrFieldException 如果传入的字段中存在不在标签集合或字段集合中的字段名，则抛出异常。
     */
    public InfluxQueryWrapper<T> select(String... fields) {
        List<String> fieldList = Arrays.asList(fields);
        fieldList.forEach(this.queryTargets::addLast);
        return this;
    }

    /**
     * 选择指定的字段以构建查询。
     * 方法会将字段添加到查询目标列表中。
     *
     * @param fields 包含多个字段的 {@code InfluxFields} 对象，用于定义查询中选择的字段集合。
     *               通过 {@code fields.getFieldKeys()} 获取字段名列表，将其添加入查询目标。
     * @return 经过字段添加后的 {@code InfluxQueryWrapper<T>} 实例，用于链式调用。
     */
    public InfluxQueryWrapper<T> select(InfluxFields fields) {
        fields.getFieldKeys().forEach(this.queryTargets::addLast);
        return this;
    }

    /**
     * 根据指定的 InfluxTags 对象设置查询所需的标签集合。
     * 通过该方法可以为查询语句添加标签作为筛选条件。
     *
     * @param tags 查询所需的标签集合，包含所有所需的键值对。
     *             标签键集合通过调用 {@code tags.getTagKeys()} 方法获取。
     * @return 当前 InfluxQueryWrapper 实例，用于链式调用。
     */
    public InfluxQueryWrapper<T> select(InfluxTags tags) {
        tags.getTagKeys().forEach(this.queryTargets::addLast);
        return this;
    }

    /**
     * 从当前实例的 tags 属性中选择标签，用作查询目标。
     * 调用该方法会将 tags 中的键添加到查询目标集合中。
     *
     * @return 返回当前 InfluxQueryWrapper 实例，便于链式调用的实现
     */
    public InfluxQueryWrapper<T> selectSelfTag() {
        return select(this.tags);
    }

    /**
     * 查询当前对象自身的字段，并将其加入查询条件中。
     * 该方法会使用当前实例持有的字段列表来构建查询。
     *
     * @return 当前 InfluxQueryWrapper 实例，用于支持链式调用
     */
    public InfluxQueryWrapper<T> selectSelfField() {
        return select(this.fields);
    }

    /**
     * 在查询目标中添加时间字段以构建查询。
     * 调用该方法后，查询结果将包含时间字段，常用于获取时间序列数据。
     *
     * @return 当前 InfluxQueryWrapper 实例，用于支持链式调用。
     */
    public InfluxQueryWrapper<T> withTime(boolean withTime) {
        this.withTime = withTime;
        return this;
    }

    // endregion

    // region 执行构建 终结方法

    /**
     * 终结方法，根据 {@link InfluxQueryWrapper#queryTargets}内指定的查询目标，
     * 构建查询语句。此方法会首先验证目标表是否配置，验证目标字段是否存在（若非匿名查询链）。
     *
     * @return 构建后的 SQL 语句
     * @throws NoSuchTagOrFieldException 如果字段校验不通过，则抛出此异常，指明无效字段。
     */
    public String build() {
        // 验证参数
        if (!validBuild()) {
            log.warn("【InfluxQueryWrapper】未构建查询语句，因为没有查询参数");
            return null;
        }
        // 验证查询目标
        validSelectFields(queryTargets);
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        while (!queryTargets.isEmpty()) {
            builder.append(queryTargets.removeFirst());
            builder.append(",");
        }
        // 添加时间
        if (withTime) {
            builder.append("time");
        } else {
            builder.deleteCharAt(builder.length() - 1);
        }
        // 指定目标表
        builder.append(" FROM ").append(measurement);
        // 初始化查询条件
        this.isConditioned = false;
        // 处理查询条件
        if (this.conditionWrapper != null && !this.conditionWrapper.parameters.isEmpty()) {
            // 验证查询条件字段是否存在
            validSelectFields(this.conditionWrapper.targets);
            // 添加查询条件
            builder.append(" WHERE (").append(this.conditionWrapper.sql()).append(")");
            this.isConditioned = true;
        }
        this.isBuild = true;
        return builder.toString().trim();
    }

    // endregion

    // region 私有方法

    /**
     * 用于构建前校验，验证当前查询参数是否为有效状态。
     * 该方法通过检查目标测量名称及查询参数来判断查询的有效性：
     * - 如果目标测量名称为空，则会尝试从引用对象中获取测量名称。
     * - 如果查询参数为空，则视为无效查询。
     *
     * @return 如果目标测量名称不为空且查询参数存在，返回 true；否则返回 false
     * @throws IllegalArgumentException 如果目标测量名称为空且无法从引用对象中获取，则抛出异常
     */
    private boolean validBuild() {
        // 目标表名若为空，则从引用对象中获取
        if (this.measurement == null) {
            String m;
            if (reference == null || (m = reference.getMeasurement()) == null) {
                throw new IllegalArgumentException("无目标表！");
            }
            return measurement(m).queryTargets.isEmpty();
        }
        // 无查询参数，则返回 false
        return !queryTargets.isEmpty();
    }

    /**
     * 验证指定的查询字段是否有效。仅在非匿名构造器时生效。
     * 方法会检查给定的字段数组是否在标签或字段集合中存在，
     * 如果存在则通过验证，如果不存在则抛出 {@link NoSuchTagOrFieldException} 异常。
     *
     * @param fields 查询的字段名称集合，用于验证这些字段是否存在于标签集合或字段集合中。
     *               如果存在字段名不存在，将会抛出异常。
     * @throws NoSuchTagOrFieldException 如果指定的字段中存在不在标签集合或字段集合中的字段名。
     */
    private void validSelectFields(Collection<String> fields) {
        // 若是匿名构造器，则不验证查询字段是否存在
        if (isLambda) {
            return;
        }
        // 获取标签、字段约束，抽象映射类确保标签、字段不会有交集
        Set<String>     tagKeys     = this.tags.getTagKeys();
        Set<String>     fieldKeys   = this.fields.getFieldKeys();
        HashSet<String> builtinKeys = new HashSet<>(List.of(InfluxClientConstant.BUILD_IN_FIELDS));
        List<String>    targets     = new ArrayList<>(fields);

        // 获取查询字段与允许字段集的交集，若交集总数量等同于查询字段数量，则查询字段全部存在
        tagKeys.retainAll(targets);
        fieldKeys.retainAll(targets);
        builtinKeys.retainAll(targets);
        int retains = tagKeys.size() + fieldKeys.size() + builtinKeys.size();
        if (retains != targets.size()) {
            // 非全部存在，拼接存在的查询字段
            tagKeys.addAll(fieldKeys);
            // 拼接内置字段
            tagKeys.addAll(builtinKeys);
            // 抽离不存在的查询字段
            targets.removeAll(tagKeys);
            throw new NoSuchTagOrFieldException(targets.toArray(new String[0]));
        }
    }

    /**
     * 初始化引用对象。
     * 如果当前引用对象为空，则根据 `measurement` 字段创建一个匿名的引用对象实例，
     * 并通过 `tableName` 方法返回该测量名称。
     * <p>
     * 逻辑步骤如下：
     * 1. 检查 `reference` 是否为 null。
     * 2. 如果为空，使用当前的 `measurement` 创建一个 `AbstractInfluxObj` 匿名实现实例。
     * 3. 调用 `loadReference` 方法进行附加的引用初始化操作。
     * <p>
     * 注意：
     * - `measurement` 字段需包含合法的表名称，否则生成的引用对象可能无法正常使用。
     * - 该方法会使用日志记录引用初始化的状态及执行过程。
     */
    private void initReference() {
        // 引用对象为空，则根据目标表创建匿名引用对象
        if (this.reference == null) {
            log.debug("【InfluxQueryWrapper】引用对象为空，尝试初始化...");
            String m = this.measurement;
            this.reference = new AbstractInfluxObj() {
                @Override
                protected String tableName() {
                    return m;
                }
            };
        }
        loadReference();
    }

    /**
     * 加载引用对象并初始化相关字段。
     * 1. 通过断言检查引用对象是否为空，若为空则抛出异常。
     * 2. 如果当前的测量名称为空，从引用对象中获取测量名称并赋值到 `measurement`。
     * 3. 从引用对象中获取标签和字段信息，分别赋值到 `tags` 和 `fields`。
     * 4. 使用日志记录加载过程的开始和结束信息，用于调试和追踪加载状态。
     * <p>
     * 注意：
     * - 引用对象 (`reference`) 是该方法运行所必备的，调用方法之前需确保其已被正确初始化。
     * - 如果 `measurement` 已经存在，将优先保留当前值，不覆盖。
     */
    private void loadReference() {
        log.debug("【InfluxQueryWrapper】加载引用对象...");
        assert this.reference != null : "引用对象为空！";
        String m = this.measurement;
        this.measurement = m == null ? this.reference.getMeasurement() : m;
        this.tags        = this.reference.getTags();
        this.fields      = this.reference.getFields();
        log.debug("【InfluxQueryWrapper】引用对象加载完毕");
    }
    // endregion

    // region 构建条件

    public ConditionWrapper where() {
        if (conditionWrapper == null) {
            conditionWrapper = ConditionWrapper.create(this);
        }
        return this.conditionWrapper;
    }

    public static class ConditionWrapper {
        @Getter
        private final Map<String, Object>   parameters;
        @Getter
        private final Set<String>           targets  = new HashSet<>();
        private final StringBuilder         builder;
        private       AtomicInteger         paramIdx = new AtomicInteger(0);
        private       InfluxQueryWrapper<?> parent;

        private ConditionWrapper() {
            parameters = new HashMap<>();
            builder    = new StringBuilder();
        }

        private static ConditionWrapper create() {
            return new ConditionWrapper();
        }

        public static ConditionWrapper create(InfluxQueryWrapper<?> parent) {
            ConditionWrapper wrapper = new ConditionWrapper();
            wrapper.parent = parent;
            return wrapper;
        }

        public ConditionWrapper and(Consumer<ConditionWrapper> consumer, boolean condition) {
            builder.append(mayDo(condition, consumer, "AND"));
            return this;
        }

        public ConditionWrapper and(Consumer<ConditionWrapper> consumer) {
            return and(consumer, true);
        }

        public ConditionWrapper or(Consumer<ConditionWrapper> consumer, boolean condition) {
            builder.append(mayDo(condition, consumer, "OR"));
            return this;
        }

        public ConditionWrapper or(Consumer<ConditionWrapper> consumer) {
            return or(consumer, true);
        }

        public ConditionWrapper eq(String column, Object value, boolean condition) {
            return condition ? appendConditionAndMask(column, value, "=") : this;
        }

        public ConditionWrapper eq(String column, Object value) {
            return eq(column, value, true);
        }

        public ConditionWrapper lt(String column, Object value, boolean condition) {
            return condition ? appendConditionAndMask(column, value, "<") : this;
        }

        public ConditionWrapper lt(String column, Object value) {
            return lt(column, value, true);
        }

        public ConditionWrapper gt(String column, Object value, boolean condition) {
            return condition ? appendConditionAndMask(column, value, ">") : this;
        }

        public ConditionWrapper gt(String column, Object value) {
            return gt(column, value, true);
        }

        public ConditionWrapper le(String column, Object value, boolean condition) {
            return condition ? appendConditionAndMask(column, value, "<=") : this;
        }

        public ConditionWrapper le(String column, Object value) {
            return le(column, value, true);
        }

        public ConditionWrapper ge(String column, Object value, boolean condition) {
            return condition ? appendConditionAndMask(column, value, ">=") : this;
        }

        public ConditionWrapper ge(String column, Object value) {
            return ge(column, value, true);
        }

        public String build() {
            return parent.build();
        }

        public String sql() {
            return builder.toString();
        }

        public InfluxQueryWrapper<?> parent() {
            return parent;
        }

        private ConditionWrapper appendConditionAndMask(String column, Object value, String sqlSegment) {
            this.builder.append(" ( ").append(column).append(" ")
                        .append(sqlSegment)
                        .append(" $").append(addColumnValueMapping(column, value)).append(" ) ");
            return this;
        }

        private String addColumnValueMapping(String column, Object value) {
            // 保存目标列名
            this.targets.add(column);
            // 获取值的参数占位名，保存占位符 - 注入值映射，避免 SQL 注入问题
            String valueName = paramName();
            this.parameters.put(valueName, value);
            return valueName;
        }

        private String mayDo(boolean condition, Consumer<ConditionWrapper> consumer, String sqlSegment) {
            if (!condition) {
                return "";
            }
            ConditionWrapper instance = create();
            // 同步匿名 wrapper 的上下文（参数数量）
            instance.paramIdx = paramIdx;
            consumer.accept(instance);
            // 获取并添加匿名 wrapper 的处理结果
            this.targets.addAll(instance.targets);
            this.parameters.putAll(instance.parameters);
            if (sqlSegment == null) {
                return instance.sql();
            }
            return " %s (%s)".formatted(sqlSegment, instance.sql());
        }

        private String paramName() {
            return "param_" + paramIdx.incrementAndGet();
        }


    }
    // endregion
}
