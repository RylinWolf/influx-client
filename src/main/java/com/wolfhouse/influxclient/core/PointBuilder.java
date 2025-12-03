package com.wolfhouse.influxclient.core;

import com.influxdb.v3.client.Point;
import com.wolfhouse.influxclient.InfluxObjValidException;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * @author Rylin Wolf
 */
@Slf4j
public class PointBuilder {
    public static <T extends AbstractInsertObj> Point build(T obj) {
        log.debug("构建对象: {}", obj);
        // 1. 验证对象
        if (!valid(obj)) {
            throw new InfluxObjValidException();
        }
        // 2. 构建 Point 并返回
        return Point.measurement(obj.getMeasurement())
                    .setFields(obj.getFieldMap())
                    .setTags(obj.getTagMap())
                    .setTimestamp(obj.getTimestamp());
    }

    public static <T extends AbstractInsertObj> List<Point> buildAll(Collection<T> objs) {
        List<Point> points = new ArrayList<>();
        for (T obj : objs) {
            points.add(build(obj));
        }
        return points;
    }

    public static <T extends AbstractInsertObj> boolean valid(T obj) {
        // 是否为 null
        if (obj == null) {
            throw new NullPointerException("对象不得为空!");
        }
        Set<String> fields = new HashSet<>(obj.getFieldKeys());
        Set<String> tags   = new HashSet<>(obj.getTagKeys());
        // 查询条件为空(字段、标签均为空)
        if (fields.isEmpty() && tags.isEmpty()) {
            throw new IllegalArgumentException("查询条件不得为空!");
        }
        // 非法格式: 字段、标签有重复字段
        fields.retainAll(tags);
        if (!fields.isEmpty()) {
            throw new IllegalArgumentException("字段和标签不得有重复!");
        }
        return true;
    }
}
