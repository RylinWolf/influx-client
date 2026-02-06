package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.comparator.NaturalComparator;
import com.wolfhouse.influxclient.constant.InfluxBuiltInTableMeta;
import com.wolfhouse.influxclient.core.InfluxConditionWrapper;
import com.wolfhouse.influxclient.core.InfluxObjMapper;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.core.PointBuilder;
import com.wolfhouse.influxclient.exception.InfluxClientInsertException;
import com.wolfhouse.influxclient.exception.InfluxClientQueryException;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxPage;
import com.wolfhouse.influxclient.pojo.InfluxResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("all")
public class InfluxClient {
    public final InfluxDBClient                                 client;
    /** 是否启用缓存 */
    @Getter
    private      Boolean                                        cacheEnabled       = false;
    /** 缓存区 */
    private      ConcurrentLinkedQueue<AbstractActionInfluxObj> cache;
    /** 缓存区刷新间隔，在缓存区未满时，根据该间隔时间将缓存区数据写入 Influx DB. 最低精度为毫秒 */
    @Setter
    @Getter
    private      Duration                                       cacheFlushInterval = Duration.ofSeconds(1);
    /** 缓存区数量，达到此数量后将会将缓存区批量插入。 */
    @Setter
    @Getter
    private      Long                                           cacheBound         = 1000L;
    /** 当前缓存区数量 */
    private      Long                                           cacheCount;
    /** 缓存插入锁 */
    private      ReentrantLock                                  cacheInsertLock;
    /** 定时任务调度器 */
    private      ScheduledThreadPoolExecutor                    scheduledThreadPool;
    /** 定时任务 */
    private      ScheduledFuture<?>                             scheduledFuture;
    /** 正在执行的异步插入数据任务 */
    private      ConcurrentLinkedQueue<CompletableFuture<Void>> insertTasks;

    /** 启用缓存区，启动缓存处理定时任务 */
    private void enableCache() {
        if (cacheEnabled) {
            return;
        }
        synchronized (cacheEnabled) {
            // 双重锁定检查
            if (cacheEnabled) {
                return;
            }
            log.debug("【InfluxClient】启用缓存区");
            // 初始化定时任务调度器
            scheduledThreadPool = new ScheduledThreadPoolExecutor(1, (r) -> new Thread(r, "influx-client-cache-flusher"));
            // 开始定时任务，处理缓存区
            scheduledFuture = scheduledThreadPool.scheduleAtFixedRate(this::handleCache, 1000, cacheFlushInterval.toMillis(), TimeUnit.MILLISECONDS);
            // 初始化缓存区
            cache = new ConcurrentLinkedQueue<>();
            // 初始化锁
            cacheInsertLock = new ReentrantLock();
            // 初始化插入任务队列
            insertTasks = new ConcurrentLinkedQueue<>();
            // 设置状态
            cacheCount   = 0L;
            cacheEnabled = true;
            log.info("【InfluxClient】缓存区已启用，定时任务已启动，缓存刷新间隔为 {} ms", cacheFlushInterval.toMillis());
        }
    }

    /** 处理缓存区，将缓存区内容保存入 InfluxDB */
    private synchronized void handleCache() {
        // 未启用缓存或缓存列表为空
        if (!cacheEnabled || cache.isEmpty()) {
            log.debug("【InfluxClient】缓存区未启用或为空，跳过处理");
            return;
        }
        // 临时缓存列表
        ArrayList<? extends AbstractActionInfluxObj> cacheList = new ArrayList<>(cache);
        cache.clear();
        log.debug("【InfluxClient】缓存区处理完成，缓存数量: {}", cacheList.size());
        cacheCount = 0L;
        // 异步执行插入任务
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> this.insertAll(cacheList));
        insertTasks.add(future);
        future.whenComplete((v, t) -> insertTasks.remove(future));
    }

    /**
     * 插入单个对象到 InfluxDB。
     *
     * @param obj 要插入的对象。
     * @param <T> 扩展自 AbstractActionInfluxObj 的对象类型。
     */
    public <T extends AbstractActionInfluxObj> void insert(@Nonnull T obj) {
        try {
            log.debug("【InfluxClient】插入单个数据，对象: {}", obj);
            client.writePoint(PointBuilder.build(obj));
        } catch (Exception e) {
            log.error("【InfluxClient】插入数据失败: {}, obj: {}", e.getMessage(), obj, e);
            throw new InfluxClientInsertException(e);
        }
    }

    /**
     * 批量插入一组继承自 AbstractActionInfluxObj 的对象。
     * 该方法会统一将提供的对象一次性插入，因此当对象集合过大时，推荐使用批量插入方法 {@link InfluxClient#insertBatch(Collection, int)}
     *
     * @param objs 要插入的对象集合。
     * @param <T>  扩展自 AbstractActionInfluxObj 的对象类型。
     */
    public <T extends AbstractActionInfluxObj> void insertAll(@Nonnull Collection<T> objs) {
        try {
            log.debug("【InfluxClient】批量插入数据，对象数量: {}", objs.size());
            client.writePoints(PointBuilder.buildAll(objs));
        } catch (Exception e) {
            log.error("【InfluxClient】插入数据失败: {}, objs: {}", e.getMessage(), objs, e);
            throw new InfluxClientInsertException(e);
        }
    }

    /**
     * 批量插入一组继承自 AbstractActionInfluxObj 的对象。
     * 此方法将根据提供的批量大小将数据分批插入，每个批次的插入操作会并行执行。
     *
     * @param <T>       扩展自 AbstractActionInfluxObj 的对象类型。
     * @param objs      要插入的对象集合。
     * @param batchSize 每个批次的最大对象数。若总数小于或等于 batchSize，则一次性插入。
     */
    public <T extends AbstractActionInfluxObj> void insertBatch(@Nonnull Collection<T> objs, int batchSize) {
        int size = objs.size();
        if (size <= batchSize) {
            insertAll(objs);
            return;
        }
        // 0. 初始化列表
        // 插入目标列表
        List<T> objList = new ArrayList<>(objs);
        // 插入任务列表
        List<CompletableFuture<Void>> futures;
        // 1. 获取批量轮次
        int batchCount = (int) Math.ceil((double) size / batchSize);
        futures = new ArrayList<>(batchCount);
        // 2. 遍历批量轮次
        for (int i = 0; i < batchCount; i++) {
            int startIndex = i * batchSize;
            int endIndex   = Math.min(startIndex + batchSize, size);
            futures.add(CompletableFuture.runAsync(() -> insertAll(objList.subList(startIndex, endIndex))));
        }
        // 3. 等待所有任务结束
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
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
    public <T extends AbstractActionInfluxObj> void insertCache(@Nonnull Collection<T> objs) {
        // 1. 数量超过缓存区上限，则直接插入
        objs = new ArrayList<>(objs);
        int size = objs.size();
        if (size >= cacheBound) {
            log.debug("【InfluxClient】批量插入缓存数量超过缓存区上限，直接插入");
            insertAll(objs);
            return;
        }
        // 启动缓存机制
        enableCache();

        cacheInsertLock.lock();
        try {
            // 2. 数量与缓存区已有数量之和超过缓存区上限，则将缓存区清空
            if (size + cacheCount > cacheBound) {
                handleCache();
            }
            // 3. 数量与缓存区已有数量之和未超过缓存区上限，添加至缓存区
            cache.addAll(objs);
            // 4. 更新缓存区数量
            cacheCount += size;
        } finally {
            cacheInsertLock.unlock();
        }
    }

    /**
     * 指定查询构造器，计算其对应的条件构造器对应匹配的数据数量
     *
     * @param wrapper 指定条件构造器
     * @param <T>     扩展自 AbstractActionInfluxObj 的对象类型
     * @return 匹配的数据数量
     */
    public <T extends AbstractActionInfluxObj> Long count(@Nonnull InfluxQueryWrapper<T> wrapper) {
        // 获取条件构造器
        InfluxConditionWrapper<T> conditions = wrapper.getConditionWrapper();
        if (conditions == null) {
            return count(wrapper.getMeasurement(), wrapper.getMeasurementQuotingDelimiter(), null, null);
        }
        return count(wrapper.getMeasurement(), wrapper.getMeasurementQuotingDelimiter(), conditions.sql(), conditions.getParameters());
    }

    public Long count(String measurement, String measurementQuotingDelimiter, String conditions, Map<String, Object> params) {
        // 基础计数语句
        StringBuilder countBuilder = new StringBuilder("select count(0) count from ")
                .append(measurementQuotingDelimiter)
                .append(measurement)
                .append(measurementQuotingDelimiter);
        // 构建条件
        if (conditions != null && !conditions.isEmpty()) {
            countBuilder.append(" where ( ")
                        .append(conditions)
                        .append(" )");
        }
        // 执行查询，获取结果并映射为 Map
        Map<String, Object> map = InfluxObjMapper.compressToMapList(doQuery(countBuilder.toString(), params),
                                                                    new LinkedHashSet<>(Set.of("count")))
                                                 .getFirst();
        Object count = map.get("count");
        if (Number.class.isAssignableFrom(count.getClass())) {
            return ((Number) count).longValue();
        }
        return Long.parseLong(count.toString());
    }

    private Stream<Object[]> doQuery(@Nonnull String sql, @Nullable Map<String, Object> parameters) {
        try {
            if (parameters != null) {
                log.debug("执行查询: {}\n参数集: {}", sql, parameters);
                return client.query(sql, parameters);
            }
            log.debug("执行查询: {}", sql);
            return client.query(sql);
        } catch (Exception e) {
            log.error("【Influx Client】执行查询失败: {}", e.getMessage(), e);
            throw new InfluxClientQueryException(e);
        }
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
    public Stream<Object[]> query(@Nonnull String sql, @Nullable Map<String, Object> parameters) {
        return doQuery(sql, parameters);
    }

    /**
     * 使用给定的查询条件包装器执行查询操作，并返回查询结果流。
     * <p>
     * 该方法会进行计数、参数检查。
     *
     * @param wrapper 查询条件包装器，用于构建查询语句和获取查询参数。
     * @return 查询结果的流，每个结果为一个包含列值的数组。
     */
    public Stream<Object[]> query(@Nonnull InfluxQueryWrapper<?> wrapper) {
        if (count(wrapper) < 1) {
            return Stream.empty();
        }
        InfluxConditionWrapper<?> condition = wrapper.getConditionWrapper();
        return doQuery(wrapper.build(), condition == null ? null : condition.getParameters());
    }

    /**
     * 对于指定查询条件包装器，添加查询全部字段操作，并返回修改后的包装器
     *
     * @param wrapper 查询条件包装器
     * @return 修改后的包装器
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(@Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                 @Nullable Comparator<String> comparator) {
        List<String> columns = new ArrayList<>(getAllColumns(wrapper.getMeasurement()));
        if (columns == null || columns.isEmpty()) {
            return wrapper;
        }
        Collections.sort(columns, comparator);
        return wrapper.select(columns);
    }

    /**
     * 对于指定查询条件包装器，添加查询全部字段操作，并返回修改后的包装器
     * 使用自然排序
     *
     * @param wrapper 查询条件包装器
     * @return 修改后的包装器
     */
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(@Nonnull InfluxQueryWrapper<T> wrapper) {
        return addQueryAll(wrapper, Comparator.naturalOrder());
    }


    /**
     * 查询表中的全部字段，并使用给定的查询条件包装器执行查询操作。
     * 返回查询结果列表。
     * 结果会按照列名自然排序
     *
     * @param influxQueryWrapper 条件构建器
     * @return 包含表中全部字段的结果列表
     */
    public List<Map<String, Object>> queryAll(@Nonnull InfluxQueryWrapper<?> influxQueryWrapper) {
        return queryMap(addQueryAll(influxQueryWrapper));
    }

    /**
     * 对于指定查询结果列表，使用指定排序器进行排序，返回排序后的结果
     *
     * @param maps       查询结果列表
     * @param comparator 排序器
     * @return 排序后的结果列表
     */
    public List<SequencedMap<String, Object>> sortResults(@Nonnull List<Map<String, Object>> maps, @Nonnull Comparator<String> comparator) {
        return maps.stream().map(m -> {
            SequencedMap<String, Object> map = new LinkedHashMap<>(m.size());
            LinkedHashSet<String> keySet = new LinkedHashSet<>(m.keySet())
                    .stream()
                    .sorted(comparator)
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            for (String s : keySet) {
                map.putLast(s, m.get(s));
            }
            return map;
        }).toList();
    }

    /**
     * 对于指定查询结果列表，使用本项目自定义的自然排序器 {@link NaturalComparator} 进行排序，返回排序后的结果
     *
     * @param maps 查询结果列表
     * @return 排序后的结果列表
     */
    public List<SequencedMap<String, Object>> sortResults(@Nonnull List<Map<String, Object>> maps) {
        return sortResults(maps, new NaturalComparator());
    }

    /**
     * 对于指定测量表，获取其全部列名
     *
     * @param measurement 测量表名称
     * @return 列名列表
     */
    private List<String> getAllColumns(@Nonnull String measurement) {
        List<Map<String, Object>> maps = queryMap(InfluxQueryWrapper.create(InfluxBuiltInTableMeta.COLUMN_META_MEASUREMENT)
                                                                    .select(InfluxBuiltInTableMeta.COLUMN_META_COLUMN_NAME)
                                                                    .setMeasurementQuotingDelimiter("")
                                                                    .withTime(false)
                                                                    .where()
                                                                    .eq(InfluxBuiltInTableMeta.COLUMN_META_TABLE_NAME_FIELD, measurement)
                                                                    .parent());
        if (maps.isEmpty()) {
            log.warn("【InfluxClient】无法获取表 {} 的列信息，是否为空？", measurement);
            return List.of();
        }
        return maps.stream().map(m -> String.valueOf(m.get(InfluxBuiltInTableMeta.COLUMN_META_COLUMN_NAME))).toList();
    }

    /**
     * 基本的查询方法，使用给定的查询条件包装器执行查询操作，并返回查询结果流。
     *
     * @param wrapper 查询条件包装器，用于构建查询语句和获取查询参数。
     * @return 查询结果的流，每个结果为一个包含列值的数组。
     */
    private Stream<Object[]> doQuery(@Nonnull InfluxQueryWrapper<?> wrapper) {
        InfluxConditionWrapper<?> condition = wrapper.getConditionWrapper();
        return doQuery(wrapper.build(), condition == null ? null : condition.getParameters());
    }

    /**
     * 使用给定的查询条件包装器和指定的目标类，将查询结果映射为指定类型的集合。
     *
     * @param <E>     目标类型，必须继承自 AbstractBaseInfluxObj。
     * @param wrapper 查询条件包装器，用于构建查询条件。
     * @param clazz   目标类的类型信息，用于映射查询结果。
     * @return 映射后的目标类型集合。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> Collection<E> queryMap(@Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                                       @Nonnull Class<E> clazz) {
        List<Object[]> res = query(wrapper).toList();
        if (res.isEmpty()) {
            return Collections.emptyList();
        }
        return InfluxObjMapper.mapAll(res.stream(), clazz, wrapper);
    }

    /**
     * 根据给定的查询条件包装器执行查询，并将结果转换为包含键值对的列表形式返回。
     *
     * @param wrapper 查询条件包装器，包含查询的条件和参数，用于构建查询语句和设置查询参数。
     * @return 查询结果的列表，每个列表项为一个映射，表示查询结果中的各列及其对应的值。
     */
    public List<Map<String, Object>> queryMap(@Nonnull InfluxQueryWrapper<?> wrapper) {
        List<Object[]> res = query(wrapper).toList();
        if (res.isEmpty()) {
            return Collections.emptyList();
        }
        return InfluxObjMapper.compressToMapList(res.stream(), wrapper);
    }

    /**
     * 执行查询操作，根据提供的查询条件封装器返回查询结果，并使用结果映射工具处理查询结果。
     *
     * @param wrapper 查询条件包装器，用于构建查询条件和提供查询参数。
     * @return 查询结果的封装对象，包含查询执行后的数据。
     */
    @Nullable
    public InfluxResult queryResult(@Nonnull InfluxQueryWrapper<?> wrapper) {
        List<Object[]> res = query(wrapper).toList();
        if (res.isEmpty()) {
            return new InfluxResult();
        }
        return InfluxObjMapper.mapAllToResult(res.stream(), wrapper.getMixedTargetsWithAlias());
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
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> InfluxPage<E> pagination(@Nonnull InfluxQueryWrapper<T> wrapper,
                                                                                                         @Nonnull Class<E> clazz,
                                                                                                         long pageNum,
                                                                                                         long pageSize) {
        // 验证分页参数
        assert pageNum > 0 && pageSize > 0 : "分页参数配置有误，必须大于等于 0";
        // 构建分页结果
        InfluxPage<E> page = InfluxPage.<E>builder()
                                       .total(count(wrapper))
                                       .pageNum(pageNum)
                                       .pageSize(pageSize)
                                       .records(Collections.emptyList())
                                       .build();
        if (page.total() < 1) {
            return page;
        }
        // 添加分页参数: 仅当分页参数全不为 0 时
        boolean doPage = pageNum != 0 && pageSize != 0;
        if (doPage) {
            wrapper.modify()
                   .limit(pageSize, (pageNum - 1) * pageSize);
        }
        // 执行查询，设置结果集
        page.records(queryMap(wrapper, clazz).stream().toList());
        // 当没有进行分页时，更新 page 的分页参数信息
        if (!doPage) {
            page.pageNum(1);
            page.pageSize(page.records().size());
        }
        return page;
    }

    /**
     * 关闭 InfluxDB 客户端连接，确保资源释放。
     */
    public void close() {
        try {
            this.scheduledThreadPool.shutdown();
            this.scheduledFuture.cancel(true);
            handleCache();
            this.client.close();
            CompletableFuture.allOf(insertTasks.toArray(CompletableFuture[]::new)).join();
        } catch (Exception ignored) {
        }
    }

    /**
     * 判断所有异步插入任务是否已完成
     *
     * @return true: 所有任务已完成，false: 有任务未完成
     */
    public boolean isInsertTaskAllDone() {
        return insertTasks.isEmpty();
    }
}
