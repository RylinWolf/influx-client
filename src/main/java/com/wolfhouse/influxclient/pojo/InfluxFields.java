package com.wolfhouse.influxclient.pojo;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/**
 * InfluxDB 存储对象字段集合
 *
 * @author Rylin Wolf
 */
@Slf4j
@ToString(includeFieldNames = false)
public class InfluxFields {
    private final LinkedHashMap<String, Object> fields;

    private InfluxFields(Map<String, Object> fields) {
        this.fields = new LinkedHashMap<>(fields);
    }

    public InfluxFields() {
        this.fields = new LinkedHashMap<>();
    }

    @SafeVarargs
    public static InfluxFields of(Map.Entry<String, Object>... fields) {
        return new InfluxFields(new LinkedHashMap<>(Map.ofEntries(fields)));
    }

    public static InfluxFields of(Map<String, Object> fields) {
        return new InfluxFields(fields);
    }

    public static InfluxFields instance() {
        return new InfluxFields();
    }

    public static InfluxFields from(String key, Object value) {
        return InfluxFields.instance().add(key, value);
    }

    public InfluxFields add(String key, Object value) {
        fields.putLast(key, value);
        return this;
    }

    /**
     * 批量添加字段到当前对象中。
     * 如果输入的字段 Map 非 {@code LinkedHashMap} 类型，将发出警告。
     *
     * @param fields 要添加的字段集合，键为字段名，值为字段值，不能为 {@code null}。
     * @return 当前对象实例，支持链式调用。
     * @throws AssertionError 如果参数 {@code fields} 为 {@code null}。
     */
    public InfluxFields addAll(Map<String, Object> fields) {
        assert fields != null : "【InfluxFields】要填加的字段不得为 null";
        if (fields.isEmpty()) {
            return this;
        }
        if (!(LinkedHashMap.class.isAssignableFrom(fields.getClass()))) {
            log.warn("【InfluxFields】要添加的字段是无序的。若是查询操作，应使用有序 Map，否则可能导致结果集映射出错！");
        }
        // LinkedHashMap 重写了 forEach，因此无需特殊区分
        fields.forEach(this::add);
        return this;
    }

    public Object get(String key) {
        return fields.get(key);
    }

    public Map.Entry<String, Object> getFirst() {
        return fields.firstEntry();
    }

    public boolean isEmpty() {
        return fields.isEmpty();
    }

    public int size() {
        return fields.size();
    }

    public Object remove(String key) {
        return fields.remove(key);
    }

    public boolean containsKey(String key) {
        return fields.containsKey(key);
    }

    public LinkedHashMap<String, Object> toMap() {
        return new LinkedHashMap<>(fields);
    }

    public LinkedHashSet<String> getFieldKeys() {
        return new LinkedHashSet<>(fields.sequencedKeySet());
    }
}
