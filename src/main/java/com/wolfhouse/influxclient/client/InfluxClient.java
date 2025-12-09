package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.core.ConditionWrapper;
import com.wolfhouse.influxclient.core.InfluxObjMapper;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.core.PointBuilder;
import com.wolfhouse.influxclient.exception.InfluxClientInsertException;
import com.wolfhouse.influxclient.exception.InfluxClientQueryException;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxPage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InfluxClient {
    public final InfluxDBClientImpl client;

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
        ConditionWrapper<T> conditions = wrapper.getConditionWrapper();
        if (conditions == null) {
            return count(wrapper.getMeasurement(), null, null);
        }
        return count(wrapper.getMeasurement(), conditions.sql(), conditions.getParameters());
    }

    public Long count(String measurement, String conditions, Map<String, Object> params) {
        // 基础计数语句
        StringBuilder countBuilder = new StringBuilder("select count(0) count from `")
                .append(measurement)
                .append("`");
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
        ConditionWrapper<?> condition = wrapper.getConditionWrapper();
        return doQuery(wrapper.build(), condition == null ? null : condition.getParameters());
    }

    /**
     * 基本的查询方法，使用给定的查询条件包装器执行查询操作，并返回查询结果流。
     *
     * @param wrapper 查询条件包装器，用于构建查询语句和获取查询参数。
     * @return 查询结果的流，每个结果为一个包含列值的数组。
     */
    private Stream<Object[]> doQuery(InfluxQueryWrapper<?> wrapper) {
        ConditionWrapper<?> condition = wrapper.getConditionWrapper();
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
        return InfluxObjMapper.mapAll(query(wrapper), clazz, wrapper);
    }

    /**
     * 根据给定的查询条件包装器执行查询，并将结果转换为包含键值对的列表形式返回。
     *
     * @param wrapper 查询条件包装器，包含查询的条件和参数，用于构建查询语句和设置查询参数。
     * @return 查询结果的列表，每个列表项为一个映射，表示查询结果中的各列及其对应的值。
     */
    public List<Map<String, Object>> queryMap(InfluxQueryWrapper<?> wrapper) {
        return InfluxObjMapper.compressToMapList(query(wrapper), wrapper);
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
}
