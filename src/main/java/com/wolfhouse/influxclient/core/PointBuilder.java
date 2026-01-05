package com.wolfhouse.influxclient.core;

import com.influxdb.v3.client.Point;
import com.wolfhouse.influxclient.exception.InfluxObjValidException;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @author Rylin Wolf
 */
@Slf4j
public class PointBuilder {
    /**
     * 构建一个与指定InfluxDB数据对象相关联的Point实例。
     * 首先验证传入对象是否合法，如果验证失败则抛出异常。
     * 验证通过后，根据对象的测量值、字段、标签及时间戳，构建并返回一个Point对象。
     *
     * @param <T> 继承自AbstractActionInfluxObj的InfluxDB数据对象类型
     * @param obj 要进行构建的InfluxDB数据对象
     * @return 构建的Point实例
     * @throws InfluxObjValidException  如果对象验证未通过
     * @throws NullPointerException     如果传入的对象为null
     * @throws IllegalArgumentException 如果对象的字段和标签中存在重复的键
     */
    public static <T extends AbstractActionInfluxObj> Point build(T obj) {
        log.debug("【PointBuilder】构建对象: {}", obj);
        // 1. 验证对象
        if (!valid(obj)) {
            throw new InfluxObjValidException();
        }
        // 2. 构建 Point 并返回
        return Point.measurement(obj.measurement())
                    .setFields(obj.getFieldMap())
                    .setTags(obj.getTagMap())
                    .setTimestamp(obj.time());
    }

    /**
     * 构建并返回一个包含多个Point实例的列表，每个Point实例与输入的InfluxDB数据对象集合中的一个对象对应。
     * 调用此方法前会对传入对象集合中的每个对象进行验证，验证通过后执行Point构建操作。
     *
     * @param <T>  继承自AbstractActionInfluxObj的InfluxDB数据对象类型
     * @param objs 要构建Point实例的InfluxDB数据对象集合
     * @return 一个包含所有构建完成的Point实例的列表
     * @throws InfluxObjValidException  如果对象验证未通过
     * @throws NullPointerException     如果输入集合或其中的对象为null
     * @throws IllegalArgumentException 如果对象集合中的对象字段和标签中有重复的键
     */
    public static <T extends AbstractActionInfluxObj> List<Point> buildAll(Collection<T> objs) {
        List<Point> points = new ArrayList<>();
        for (T obj : objs) {
            points.add(build(obj));
        }
        return points;
    }

    public static <T extends AbstractActionInfluxObj> boolean valid(T obj) {
        // 是否为 null
        if (obj == null) {
            throw new NullPointerException("【PointBuilder】对象不得为空!");
        }
        Set<String> fields = new HashSet<>(obj.getFieldKeys());
        Set<String> tags   = new HashSet<>(obj.getTagKeys());
        // 内容为空(字段、标签均为空)
        if (fields.isEmpty() && tags.isEmpty()) {
            throw new IllegalArgumentException("【PointBuilder】内容 (标签/字段) 不得全为空!");
        }
        // 非法格式: 字段、标签有重复字段
        fields.retainAll(tags);
        if (!fields.isEmpty()) {
            throw new IllegalArgumentException("【PointBuilder】字段和标签不得有重复!");
        }
        return true;
    }
}
