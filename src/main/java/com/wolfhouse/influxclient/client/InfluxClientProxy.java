package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.comparator.NaturalComparator;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxPage;
import com.wolfhouse.influxclient.pojo.InfluxResult;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import jakarta.annotation.Nonnull;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

/**
 * InfluxClient 代理类
 *
 * @author Rylin Wolf
 */
@Slf4j
@SuppressWarnings({"unused"})
public class InfluxClientProxy {
    @Getter
    @Accessors(fluent = true)
    private InfluxClient       client;
    private InfluxDBClientImpl influxDbClient;

    private InfluxClientProxy() {
    }

    public InfluxClientProxy(@Nonnull InfluxDBClientImpl dbClient, @Nonnull InfluxClient client) {
        this.influxDbClient = dbClient;
        this.client         = client;
    }

    @Nullable
    public static InfluxClientProxy instance(@Nonnull InfluxDbProperties properties) {
        log.info("[InfluxClientProxy] 创建实例");
        log.info("[InfluxClientProxy] 正在初始化 Influx 客户端... {}", properties.getUrl());
        String token = properties.getToken();
        if (token == null || token.isEmpty()) {
            log.error("[InfluxClientProxy] Token为空，Influx 客户端将不会被初始化");
            return null;
        }
        try {
            InfluxDBClientImpl dbClient     = newDbClient(properties);
            InfluxClient       influxClient = newClient(dbClient, properties);
            log.info("[InfluxClientProxy] Influx 客户端初始化完成，版本: {}", dbClient.getServerVersion());
            return new InfluxClientProxy(dbClient, influxClient);
        } catch (Exception e) {
            log.error("[InfluxClientProxy] Influx 客户端初始化失败: {}, properties: {}", e.getMessage(), properties, e);
            return null;
        }
    }

    /**
     * 根据给定配置，创建新的 InfluxDBClient 实例
     *
     * @param properties 配置
     * @return {@link InfluxDBClient} 实例
     */
    @Nonnull
    public static InfluxDBClientImpl newDbClient(@Nonnull InfluxDbProperties properties) {
        return (InfluxDBClientImpl) InfluxDBClient.getInstance(
                properties.getUrl(), properties.getToken().toCharArray(), properties.getDatabase());
    }

    /**
     * 根据给定的 {@link InfluxDBClientImpl} 实例 以及 {@link InfluxDbProperties} 配置，创建新的 {@link InfluxClient} 实例
     *
     * @param dbClient   {@link InfluxDBClientImpl} 实例
     * @param properties 配置
     * @return {@link InfluxClient} 实例
     */
    @Nonnull
    public static InfluxClient newClient(@Nonnull InfluxDBClientImpl dbClient, @Nonnull InfluxDbProperties properties) {
        InfluxClient influxClient = new InfluxClient(dbClient);
        influxClient.setCacheBound(properties.getCacheBound());
        influxClient.setCacheFlushInterval(Duration.ofMillis(properties.getCacheFlushInterval()));
        return influxClient;
    }

    /**
     * 根据给定的配置，刷新 {@link InfluxDBClientImpl}, {@link InfluxClient} 实例
     *
     * @param properties {@link InfluxDbProperties}配置
     */
    public void refreshClient(@Nonnull InfluxDbProperties properties) {
        synchronized (this) {
            log.debug("[InfluxClient] 刷新客户端");
            try {
                this.client.close();
            } catch (Exception e) {
                log.error("关闭 InfluxClient 实例时发生错误", e);
            }
            this.influxDbClient = newDbClient(properties);
            this.client         = newClient(this.influxDbClient, properties);
            log.info("[InfluxClient] 客户端刷新完成");
        }
    }
    // region InfluxDbClient 代理方法

    /**
     * 获取当前 InfluxDbClient 版本号
     *
     * @return 版本号
     */
    public String getServerVersion() {
        return this.influxDbClient.getServerVersion();
    }
    // endregion

    // region Influx Client 代理方法

    /** 启用缓存区，启动缓存处理定时任务 */
    public void enableCache() {
        client.enableCache();
    }

    /** 处理缓存区，将缓存区内容保存入 InfluxDB */
    public void handleCache() {
        client.handleCache();
    }

    /**
     * 插入单个对象到 InfluxDB。
     *
     * @param obj 要插入的对象。
     * @param <T> 扩展自 AbstractActionInfluxObj 的对象类型。
     */
    public <T extends AbstractActionInfluxObj> void insert(@Nonnull T obj) {
        client.insert(obj);
    }

    /**
     * 批量插入一组继承自 AbstractActionInfluxObj 的对象。
     * 该方法会统一将提供的对象一次性插入，因此当对象集合过大时，推荐使用批量插入方法 {@link InfluxClient#insertBatch(Collection, int)}
     *
     * @param objs 要插入的对象集合。
     * @param <T>  扩展自 AbstractActionInfluxObj 的对象类型。
     */
    public <T extends AbstractActionInfluxObj> void insertAll(@javax.annotation.Nonnull Collection<T> objs) {
        client.insertAll(objs);
    }


    /**
     * 批量插入一组继承自 AbstractActionInfluxObj 的对象。
     * 此方法将根据提供的批量大小将数据分批插入，每个批次的插入操作会并行执行。
     *
     * @param <T>       扩展自 AbstractActionInfluxObj 的对象类型。
     * @param objs      要插入的对象集合。
     * @param batchSize 每个批次的最大对象数。若总数小于或等于 batchSize，则一次性插入。
     */
    public <T extends AbstractActionInfluxObj> void insertBatch(@javax.annotation.Nonnull Collection<T> objs, int batchSize) {
        client.insertBatch(objs, batchSize);
    }

    /**
     * 批量插入一组继承自 AbstractActionInfluxObj 的对象到缓存区。
     * 当对象集合数量大于或等于缓存区上限时，直接插入；否则，执行缓存机制，根据缓存区当前状态决定操作。
     * 若累计到的缓存数量超过缓存区上限，则清空缓存区并插入数据库。
     * 此方法适用于需要批量插入并利用缓存机制提升性能的场景。
     *
     * @param <T>  扩展自 AbstractActionInfluxObj 的对象类型。
     * @param objs 待插入的对象集合，不能为空。
     */
    public <T extends AbstractActionInfluxObj> void insertCache(@javax.annotation.Nonnull Collection<T> objs) {
        client.insertCache(objs);
    }

    /**
     * 指定查询构造器，计算其对应的条件构造器对应匹配的数据数量
     *
     * @param wrapper 指定条件构造器
     * @param <T>     扩展自 AbstractActionInfluxObj 的对象类型
     * @return 匹配的数据数量
     */
    public <T extends AbstractActionInfluxObj> Long count(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper) {
        return client.count(wrapper);
    }

    /**
     * 执行给定的SQL查询，并使用参数化查询返回结果流。
     * <p>
     * 该方法直接调用 SQL，因此不会进行计数检查和参数检查。建议使用 {@link InfluxClient#query(InfluxQueryWrapper)} 代替。
     *
     * @param sql        执行的SQL查询语句，定义了要检索的数据。
     * @param parameters 查询的参数集合，以键值对的形式提供参数和值，用于填充SQL语句中的占位符。
     * @return 包含查询结果的流，每个结果是一个包含列值的数组。
     */
    public Stream<Object[]> query(@javax.annotation.Nonnull String sql, @Nullable Map<String, Object> parameters) {
        return client.query(sql, parameters);
    }

    /**
     * 使用给定的查询条件包装器执行查询操作，并返回查询结果流。
     * <p>
     * 该方法会进行计数、参数检查。
     *
     * @param wrapper 查询条件包装器，用于构建查询语句和获取查询参数。
     * @return 查询结果的流，每个结果为一个包含列值的数组。
     */
    public Stream<Object[]> query(@javax.annotation.Nonnull InfluxQueryWrapper<?> wrapper) {
        return client.query(wrapper);
    }


    /**
     * 使用给定的查询条件包装器执行查询操作，并返回查询结果流。
     * <p>
     * 该方法会进行计数、参数检查。
     *
     * @param wrapper    查询条件包装器，用于构建查询语句和获取查询参数。
     * @param countCheck 是否进行计数检查，如果为true，则在执行查询前检查计数是否为 0，为 0 则返回空流。
     * @return 查询结果的流，每个结果为一个包含列值的数组。
     */
    public Stream<Object[]> query(@javax.annotation.Nonnull InfluxQueryWrapper<?> wrapper, boolean countCheck) {
        return client.query(wrapper, countCheck);
    }

    /**
     * 对于指定查询条件包装器，添加查询全部字段操作，并返回修改后的包装器
     *
     * @param wrapper 查询条件包装器
     * @return 修改后的包装器
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                 @Nullable Comparator<String> comparator) {
        return client.addQueryAll(wrapper, comparator);
    }

    /**
     * 对于指定查询包装器，执行添加查询全部字段操作，并返回修改后的包装器。
     * <p>
     * 从包装器中获取 measurement，并获取其中的所有列，作为查询目标。
     * 使用自然排序
     *
     * @param wrapper 查询条件包装器
     * @return 修改后的包装器
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper) {
        return client.addQueryAll(wrapper);
    }

    /**
     * 对于指定查询结果列表，使用指定排序器进行排序，返回排序后的结果
     *
     * @param maps       查询结果列表
     * @param comparator 排序器
     * @return 排序后的结果列表
     */
    public List<SequencedMap<String, Object>> sortResults(@javax.annotation.Nonnull List<Map<String, Object>> maps, @javax.annotation.Nonnull Comparator<String> comparator) {
        return client.sortResults(maps, comparator);
    }

    /**
     * 对于指定测量表，获取其全部列名
     *
     * @param measurement 测量表名称
     * @return 列名列表
     */
    public List<String> tableColumns(@javax.annotation.Nonnull String measurement) {
        return client.tableColumns(measurement);
    }

    /**
     * 对于指定查询结果列表，使用本项目自定义的自然排序器 {@link NaturalComparator} 进行排序，返回排序后的结果
     *
     * @param maps 查询结果列表
     * @return 排序后的结果列表
     */
    public List<SequencedMap<String, Object>> sortResults(@javax.annotation.Nonnull List<Map<String, Object>> maps) {
        return client.sortResults(maps);
    }

    /**
     * 获取当前数据库下的所有表名
     *
     * @return 表名列表
     */
    public List<String> tableNames() {
        return client.tableNames();
    }

    /**
     * 向指定的 wrapper 中添加查询条件：按照时间排序，最近/最早的一次记录。
     * <p>
     * 该方法执行完毕后，指定的 wrapper 中会根据特定字段分组，并按照时间排序获取最近/最早的一次记录。
     * 记录列仅会包含指定的 groupBy 列。
     * <p>
     * 使用该方法构造后的 wrapper 不应再添加其他查询的列名。
     * <p>
     * 例: addRecent(wrapper, true, "field1", "field2") 在执行完毕后，wrapper 的内容为:
     * <p>
     * SELECT field1, field2, max(timestamp) as recent_time FROM table_name GROUP BY field1, field2
     *
     * @param desc    是否降序，默认为 true（最近一次），若 false 则为最早一次
     * @param groupBy 分组列，查询基准
     * @return 当前 InfluxQueryWrapper 实例
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addRecent(InfluxQueryWrapper<T> parent,
                                                                               Boolean desc,
                                                                               String... groupBy) {
        return client.addRecent(parent, desc, groupBy);
    }

    /**
     * 向指定的 wrapper 中添加查询条件：按照时间排序，最近的一次记录。
     * <p>
     * 用法见 {@link #addRecent(InfluxQueryWrapper, Boolean, String...)}
     *
     * @param queryFields 查询列
     * @return 当前 InfluxQueryWRapper 实例
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addRecent(InfluxQueryWrapper<T> parent,
                                                                               String... queryFields) {
        return client.addRecent(parent, queryFields);
    }

    /**
     * 向指定查询构造器中添加条件，用于查询最近的数据。
     * <p>
     * 该方法执行后，会进行以下操作：
     * - 创建最近查询构造器，通过 {@link InfluxClient#addRecent(InfluxQueryWrapper, Boolean, String...)} 添加最近查询条件
     * - 执行查询，获得所有分组的最近/最早记录
     * - 根据记录，构造查询这些记录完整数据的条件
     *
     * @param parent      父查询器，表示需要添加最近查询功能的查询器。
     * @param desc        是否按降序排列，true 表示降序，false 表示升序。
     * @param queryFields 查询字段列表，用于指定需要匹配的字段。
     * @return 构造完成的查询器，用于执行包含最近数据条件的查询。
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryRecent(InfluxQueryWrapper<T> parent,
                                                                                    boolean desc,
                                                                                    String... queryFields) {
        return client.addQueryRecent(parent, desc, queryFields);
    }

    /**
     * 使用给定的查询条件包装器和指定的目标类，将查询结果映射为指定类型的集合。
     *
     * @param <E>     目标类型，必须继承自 AbstractBaseInfluxObj。
     * @param wrapper 查询条件包装器，用于构建查询条件。
     * @param clazz   目标类的类型信息，用于映射查询结果。
     * @return 映射后的目标类型集合。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> List<E> queryMap(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                                 @javax.annotation.Nonnull Class<E> clazz) {
        return client.queryMap(wrapper, clazz);
    }

    /**
     * 根据给定的查询条件包装器执行查询，并将结果转换为包含键值对的列表形式返回。
     *
     * @param wrapper 查询条件包装器，包含查询的条件和参数，用于构建查询语句和设置查询参数。
     * @return 查询结果的列表，每个列表项为一个映射，表示查询结果中的各列及其对应的值。
     */
    public List<Map<String, Object>> queryMap(@javax.annotation.Nonnull InfluxQueryWrapper<?> wrapper) {
        return client.queryMap(wrapper);
    }

    /**
     * 根据给定的查询条件包装器执行查询，并将结果转换为包含键值对的列表形式返回。
     *
     * @param wrapper    查询条件包装器，包含查询的条件和参数，用于构建查询语句和设置查询参数。
     * @param countCheck 是否进行计数检查，如果为true，则在执行查询前检查计数是否为 0，为 0 则返回空流。
     * @return 查询结果的列表，每个列表项为一个映射，表示查询结果中的各列及其对应的值。
     */
    public List<Map<String, Object>> queryMap(@javax.annotation.Nonnull InfluxQueryWrapper<?> wrapper, boolean countCheck) {
        return client.queryMap(wrapper, countCheck);
    }

    /**
     * 执行查询操作，根据提供的查询条件封装器返回查询结果，并使用结果映射工具处理查询结果。
     *
     * @param wrapper 查询条件包装器，用于构建查询条件和提供查询参数。
     * @return 查询结果的封装对象，包含查询执行后的数据。
     */
    @Nullable
    public InfluxResult queryResult(@javax.annotation.Nonnull InfluxQueryWrapper<?> wrapper) {
        return client.queryResult(wrapper);
    }

    /**
     * 对给定的查询条件进行分页查询，返回指定类型的分页结果。
     *
     * @param <E>      数据对象的类型，必须继承自 AbstractBaseInfluxObj。
     * @param wrapper  查询条件包装器，用于构建查询的条件和参数。
     * @param clazz    数据对象的目标类型，用于映射查询结果。
     * @param pageNum  当前页码，设为 0 则不限制查询结果数
     * @param pageSize 每页显示的数据条数，设为 0 则不限制查询结果数
     * @return 包含查询结果的分页对象，包含总记录数、页码、每页大小以及当前页的数据。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> InfluxPage<E>
    pagination(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper,
               @javax.annotation.Nonnull Class<E> clazz,
               long pageNum,
               long pageSize) {
        return client.pagination(wrapper, clazz, pageNum, pageSize);
    }

    /**
     * 对给定的查询条件进行分页查询，返回指定类型的分页结果。
     *
     * @param <E>      数据对象的类型，必须继承自 AbstractBaseInfluxObj。
     * @param wrapper  查询条件包装器，用于构建查询的条件和参数。
     * @param clazz    数据对象的目标类型，用于映射查询结果。
     * @param pageNum  当前页码，设为 0 则不限制查询结果数
     * @param pageSize 每页显示的数据条数，设为 0 则不限制查询结果数
     * @param offset   分页偏移量，用于跳过指定数量的记录
     * @return 包含查询结果的分页对象，包含总记录数、页码、每页大小以及当前页的数据。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> InfluxPage<E>
    pagination(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper,
               @javax.annotation.Nonnull Class<E> clazz,
               long pageNum,
               long pageSize,
               long offset) {
        return client.pagination(wrapper, clazz, pageNum, pageSize, offset);
    }

    /**
     * 使用给定的查询条件包装器和指定的目标类，将查询结果映射为指定类型的集合。
     *
     * @param <E>        目标类型，必须继承自 AbstractBaseInfluxObj。
     * @param wrapper    查询条件包装器，用于构建查询条件。
     * @param clazz      目标类的类型信息，用于映射查询结果。
     * @param countCheck 是否检查查询结果数量，如果为true，则在查询结果为空时返回空列表。开启该选项会强制查询前获取数据数量。
     * @return 映射后的目标类型集合。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> List<E> queryMap(@javax.annotation.Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                                 @javax.annotation.Nonnull Class<E> clazz,
                                                                                                 boolean countCheck) {
        return client.queryMap(wrapper, clazz, countCheck);
    }


    /**
     * 查询表中的全部字段，并使用给定的查询条件包装器执行查询操作。
     * 返回查询结果列表。
     * 结果会按照列名自然排序
     *
     * @param influxQueryWrapper 条件构建器
     * @return 包含表中全部字段的结果列表
     */
    public List<Map<String, Object>> queryAll(@javax.annotation.Nonnull InfluxQueryWrapper<?> influxQueryWrapper) {
        return client.queryAll(influxQueryWrapper);
    }

    /**
     * 关闭 InfluxDB 客户端连接，确保资源释放。
     */
    public void close() {
        client.close();
    }

    /**
     * 判断所有异步插入任务是否已完成
     *
     * @return true: 所有任务已完成，false: 有任务未完成
     */
    public boolean isInsertTaskAllDone() {
        return client.isInsertTaskAllDone();
    }
    // endregion
}
