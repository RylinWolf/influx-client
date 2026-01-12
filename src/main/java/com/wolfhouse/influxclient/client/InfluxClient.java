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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("all")
public class InfluxClient {
    public final InfluxDBClient client;

    public <T extends AbstractActionInfluxObj> void insert(T obj) {
        try {
            client.writePoint(PointBuilder.build(obj));
        } catch (Exception e) {
            log.error("【InfluxClient】插入数据失败: {}", e.getMessage(), e);
            throw new InfluxClientInsertException(e);
        }
    }

    public <T extends AbstractActionInfluxObj> void insertAll(Collection<T> objs) {
        try {
            client.writePoints(PointBuilder.buildAll(objs));
        } catch (Exception e) {
            log.error("【InfluxClient】插入数据失败: {}", e.getMessage(), e);
            throw new InfluxClientInsertException(e);
        }
    }

    public <T extends AbstractActionInfluxObj> Long count(InfluxQueryWrapper<T> wrapper) {
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

    private Stream<Object[]> doQuery(String sql, Map<String, Object> parameters) {
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
    public Stream<Object[]> query(String sql, Map<String, Object> parameters) {
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
    public Stream<Object[]> query(InfluxQueryWrapper<?> wrapper) {
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
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(InfluxQueryWrapper<T> wrapper, Comparator<String> comparator) {
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
    public <T extends AbstractActionInfluxObj> InfluxQueryWrapper<T> addQueryAll(InfluxQueryWrapper<T> wrapper) {
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
    public List<Map<String, Object>> queryAll(InfluxQueryWrapper<?> influxQueryWrapper) {
        return queryMap(addQueryAll(influxQueryWrapper));
    }

    /**
     * 对于指定查询结果列表，使用指定排序器进行排序，返回排序后的结果
     *
     * @param maps       查询结果列表
     * @param comparator 排序器
     * @return 排序后的结果列表
     */
    public List<SequencedMap<String, Object>> sortResults(List<Map<String, Object>> maps, Comparator<String> comparator) {
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
    public List<SequencedMap<String, Object>> sortResults(List<Map<String, Object>> maps) {
        return sortResults(maps, new NaturalComparator());
    }

    /**
     * 对于指定测量表，获取其全部列名
     *
     * @param measurement 测量表名称
     * @return 列名列表
     */
    private List<String> getAllColumns(String measurement) {
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
    private Stream<Object[]> doQuery(InfluxQueryWrapper<?> wrapper) {
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
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> Collection<E> queryMap(InfluxQueryWrapper<T> wrapper, Class<E> clazz) {
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
    public List<Map<String, Object>> queryMap(InfluxQueryWrapper<?> wrapper) {
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
    public InfluxResult queryResult(InfluxQueryWrapper<?> wrapper) {
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
     * @param pageNum  当前页码，从1开始。
     * @param pageSize 每页显示的数据条数。
     * @return 包含查询结果的分页对象，包含总记录数、页码、每页大小以及当前页的数据。
     */
    public <E extends AbstractBaseInfluxObj, T extends AbstractActionInfluxObj> InfluxPage<E> pagination(InfluxQueryWrapper<T> wrapper, Class<E> clazz, long pageNum, long pageSize) {
        // 验证分页参数
        assert pageNum > 0 && pageSize > 0 : "分页参数配置有误，必须大于 0";
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
        // 添加分页参数
        wrapper.modify()
               .limit(pageSize, (pageNum - 1) * pageSize);
        // 执行查询，设置结果集
        return page.records(queryMap(wrapper, clazz).stream().toList());
    }

    /**
     * 关闭 InfluxDB 客户端连接，确保资源释放。
     */
    public void close() {
        try {
            this.client.close();
        } catch (Exception ignored) {
        }
    }
}
