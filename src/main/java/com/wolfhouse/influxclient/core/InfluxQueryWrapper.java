package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.InfluxClientConstant;
import com.wolfhouse.influxclient.exception.NoSuchTagOrFieldException;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxFields;
import com.wolfhouse.influxclient.pojo.InfluxTags;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.*;

import static com.wolfhouse.influxclient.InfluxClientConstant.TIMESTAMP_FIELD;

/**
 * 基于 {@link AbstractActionInfluxObj} 的查询构建器。
 * 该类用于构建InfluxDB查询语句，提供流式API来构建查询条件。
 * 支持字段选择、条件过滤、时间戳包含等功能。
 *
 * @author Rylin Wolf
 */
@Slf4j
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@SuppressWarnings({"UnusedReturnValue", "unused"})
public class InfluxQueryWrapper<T extends AbstractActionInfluxObj> extends BaseSqlBuilder {
    /** 表示当前构造器是否为匿名构造器（未通过泛型类型创建） */
    private boolean                       isLambda                    = false;
    /** 控制查询结果是否包含时间戳字段 */
    private boolean                       withTime                    = true;
    /** 当前查询的目标表（measurement）名称 */
    private String                        measurement;
    /** 当前查询关联的映射对象引用，提供表结构信息 */
    private AbstractActionInfluxObj       reference;
    /** 表名标识符引用符 */
    private String                        measurementQuotingDelimiter = "`";
    /** 列名标识符引用符 */
    private String                        columnQuotingDelimiter      = "`";
    /** 当前查询涉及的标签约束集合 */
    private InfluxTags                    tags;
    /** 当前查询涉及的字段约束集合 */
    private InfluxFields                  fields;
    /** 当前查询的目标列以及别名集合 */
    private LinkedHashMap<String, String> aliasMap                    = new LinkedHashMap<>();
    /** 当前查询的目标列集合，若有别名则使用别名代替（仅在构建后有效） */
    private LinkedHashSet<String>         mixedTargetsWithAlias;
    /** 当前查询的条件构造器，用于构建WHERE子句 */
    private InfluxConditionWrapper<T>     conditionWrapper;
    /** 标记当前查询是否已经构建完成 */
    private boolean                       isBuild                     = false;
    /** 标记当前查询是否包含WHERE条件（仅在构建后有效） */
    private boolean                       isConditioned               = false;
    /** 当前查询的查询参数构造器 */
    private InfluxModifiersWrapper<T>     modifiersWrapper;
    private boolean                       isModified                  = false;

    // region 构造方法

    /**
     * 构造一个带有引用对象的 InfluxQueryWrapper 实例，并初始化引用对象。
     * 该构造方法会自动从引用对象中提取表名、标签和字段信息。
     *
     * @param reference 被包装的引用对象，用于提供查询或构建SQL的相关信息。
     *                  该对象必须是AbstractInfluxObj的子类实例，包含表名、标签和字段定义。
     */
    private InfluxQueryWrapper(@Nonnull T reference) {
        this.reference = reference;
        initReference();
    }

    /**
     * 构造一个 InfluxQueryWrapper 实例，并设置测量名称。
     * 该构造方法创建一个基于表名的查询构造器，不包含预定义的标签和字段约束。
     *
     * @param measurement 测量名称，用于指定目标数据表。
     *                    该参数不能为null或空字符串。
     */
    private InfluxQueryWrapper(@Nonnull String measurement) {
        measurement(measurement);
        initReference();
    }

    /**
     * 创建一个空的 InfluxQueryWrapper 实例。
     * 用于创建匿名查询构造器。
     */
    private InfluxQueryWrapper() {}

    // endregion

    // region 获取实例

    /**
     * 从指定的 {@link AbstractActionInfluxObj} 对象创建并初始化一个 {@link InfluxQueryWrapper} 实例。
     *
     * @param obj 提供初始化数据的对象，该对象必须继承自 {@link AbstractActionInfluxObj}。
     *            通过调用该对象的 {@code getMeasurement()} 方法获得测量名称。
     * @return 初始化好的 {@link InfluxQueryWrapper} 实例，其中包含传入对象的测量名称。
     */
    public static <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> from(@Nonnull T obj) {
        return new InfluxQueryWrapper<>(obj);
    }

    /**
     * 从指定的 {@code AbstractActionInfluxObj} 对象创建并初始化一个 {@code InfluxQueryWrapper} 实例。
     * 此方法会在创建过程中自动选择该对象的标记和字段，便于快速构造查询配置。
     *
     * @param <T> 泛型类型，必须为 {@code AbstractActionInfluxObj} 的子类，用于泛化查询的目标对象。
     * @param obj 提供初始化数据的对象，该对象包含测量名称、标记和字段信息。不能为 {@code null}，否则会引发空指针异常。
     * @return 一个基于传入对象初始化的 {@code InfluxQueryWrapper} 实例。
     */
    public static <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> fromBuild(@Nonnull T obj) {
        InfluxQueryWrapper<T> wrapper = new InfluxQueryWrapper<>(obj);
        wrapper.selectSelfTag();
        wrapper.selectSelfField();
        return wrapper;
    }

    /**
     * 根据传入的 {@link AbstractActionInfluxObj} 对象构建 SQL 查询语句。
     * 该方法首先通过 {@link #fromBuild(AbstractActionInfluxObj)} 方法构造查询对象，
     * 然后调用查询对象的 {@code build()} 方法生成最终的 SQL 查询语句。
     *
     * @param <T> 泛型类型，必须为 {@code AbstractActionInfluxObj} 的子类，用于表示查询目标对象的具体类型。
     * @param obj 提供初始化数据的对象，该对象包含测量名称、标签和字段信息。
     *            该参数不能为空，否则会引发空指针异常。
     * @return 生成的SQL查询语句，基于传入对象的测量名称、标签和字段构建。
     */
    public static <T extends AbstractActionInfluxObj> String fromBuildSql(@Nonnull T obj) {
        return fromBuild(obj).build();
    }

    /**
     * 创建一个新的 {@link InfluxQueryWrapper} 实例，未初始化任何测量名称或引用对象。
     * <p>
     * 该实例是匿名的，查询时不会进行字段校验
     *
     * @return 一个新的 {@link InfluxQueryWrapper} 实例，测量名称和引用对象均为空。
     */
    public static InfluxQueryWrapper<?> create() {
        return create("");
    }

    /**
     * 创建一个带有指定测量名称的 InfluxQueryWrapper 实例。
     * <p>
     * 该实例是匿名的，查询时不会进行字段校验
     *
     * @param measurement 测量名称，用于指定数据查询的目标表。
     * @return 返回一个初始化了测量名称的 InfluxQueryWrapper 实例。
     */
    public static InfluxQueryWrapper<?> create(@Nonnull String measurement) {
        InfluxQueryWrapper<?> wrapper = new InfluxQueryWrapper<>(measurement);
        wrapper.isLambda = true;
        return wrapper;
    }
    // endregion

    // region 设置方法

    public InfluxQueryWrapper<T> measurement(@Nonnull String measurement) {
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
        return select(Arrays.asList(fields));
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
        return this.select(fields.getFieldKeys().toArray(String[]::new));
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
        return this.select(tags.getTagKeys().toArray(String[]::new));
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
     * 查询当前对象自身的标签及字段
     *
     * @return 当前 InfluxQueryWrapper 实例，用于支持链式调用
     */
    public InfluxQueryWrapper<T> selectSelfAll() {
        return selectSelfTag().selectSelfField();
    }

    public InfluxQueryWrapper<T> select(SequencedCollection<String> targets) {
        for (String field : targets) {
            // 处理别名
            String[] s     = field.split("\\s", 2);
            String   alias = null;
            if (s.length > 1) {
                alias = s[1].trim();
                // 使用 as 作为别名分隔符
                if (alias.startsWith("as")) {
                    alias = alias.substring(2).trim();
                }
            }
            // 添加到别名映射
            this.aliasMap.put(s[0], alias);
        }
        return this;
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
     * 终结方法，根据 {@link InfluxQueryWrapper#aliasMap}的 keys 内指定的查询目标，
     * 构建查询语句。此方法会首先验证目标表是否配置，验证目标字段是否存在（若非匿名查询链）。
     *
     * @return 构建后的 SQL 语句
     * @throws NoSuchTagOrFieldException 如果字段校验不通过，则抛出此异常，指明无效字段。
     */
    @Override
    public String build() {
        String res = super.build();
        this.isBuild = res != null;
        return res;
    }

    /**
     * 构建目标字段部分的查询语句。
     *
     * @param builder 用于构建查询语句的 {@code StringBuilder} 实例
     */
    @Override
    protected void buildTarget(StringBuilder builder) {
        builder.append("SELECT ");
        // 初始化目标字段集合
        this.mixedTargetsWithAlias = new LinkedHashSet<>();
        SequencedSet<String>  queryTargets = aliasMap.sequencedKeySet();
        LinkedHashSet<String> targets      = new LinkedHashSet<>(queryTargets);
        // 若有别名，则特殊处理
        while (!targets.isEmpty()) {
            String target = targets.removeFirst();
            String alias  = aliasMap.get(target);
            builder.append(columnQuotingDelimiter)
                   .append(target)
                   .append(columnQuotingDelimiter);
            if (alias != null) {
                // 添加入目标字段集合
                mixedTargetsWithAlias.add(alias);
                // 特殊处理别名
                builder.append(" AS ").append(alias);
            } else {
                mixedTargetsWithAlias.add(target);
            }
            builder.append(",");
        }
        // 添加时间
        if (withTime && !queryTargets.contains(TIMESTAMP_FIELD)) {
            // 添加时间字段至别名映射
            aliasMap.putLast(TIMESTAMP_FIELD, null);
            // 添加时间字段至混合目标字段
            mixedTargetsWithAlias.add(TIMESTAMP_FIELD);
            builder.append(TIMESTAMP_FIELD);
        } else {
            builder.deleteCharAt(builder.length() - 1);
        }
    }

    @Override
    protected void buildFromTable(StringBuilder builder) {
        // 指定目标表
        builder.append(" FROM ")
               .append(measurementQuotingDelimiter)
               .append(measurement)
               .append(measurementQuotingDelimiter);
    }

    @Override
    protected void buildCondition(StringBuilder builder) {
        // 初始化查询条件
        this.isConditioned = false;
        // 处理查询条件
        if (this.conditionWrapper != null && !this.conditionWrapper.getParameters().isEmpty()) {
            // 验证查询条件字段是否存在
            validSelectFields(this.conditionWrapper.getTargets());
            // 添加查询条件
            builder.append(" WHERE (").append(this.conditionWrapper.sql()).append(")");
            this.isConditioned = true;
        }
    }

    @Override
    protected void buildModifies(StringBuilder builder) {
        if (this.modifiersWrapper == null) {
            return;
        }
        builder.append(" ").append(this.modifiersWrapper.toSql());
    }

    @Override
    protected boolean validate() {
        // 验证参数
        if (!validBuild()) {
            log.warn("【InfluxQueryWrapper】未构建查询语句，因为没有查询参数");
            return false;
        }
        // 验证查询目标
        validSelectFields(aliasMap.keySet());
        return true;
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
            return measurement(m).aliasMap.isEmpty();
        }
        // 无查询参数，则返回 false
        return !aliasMap.isEmpty();
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
            this.reference = new AbstractActionInfluxObj(m) {};
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

    /**
     * 如果当前条件封装对象为空，则初始化一个新的 {@code ConditionWrapper} 实例，绑定到当前对象。
     * 若已有条件封装对象，则直接返回该对象。
     *
     * @return 返回当前对象关联的 {@code ConditionWrapper} 实例，用于管理查询的条件逻辑。
     */
    public InfluxConditionWrapper<T> where() {
        if (conditionWrapper == null) {
            conditionWrapper = InfluxConditionWrapper.create(this);
        }
        return this.conditionWrapper;
    }

    // endregion

    // region 构建查询修饰符

    /**
     * 如果当前查询修饰符封装对象为空，则初始化一个新的 {@code InfluxModifiersWrapper} 实例，绑定到当前对象。
     * 若已有条件封装对象，则直接返回该对象。
     *
     * @return 返回当前对象关联的 {@code InfluxModifiersWrapper} 实例，用于添加查询修饰符。
     */
    public InfluxModifiersWrapper<T> modify() {
        if (modifiersWrapper == null) {
            modifiersWrapper = InfluxModifiersWrapper.create(this);
        }
        return this.modifiersWrapper;
    }
    // endregion
}
