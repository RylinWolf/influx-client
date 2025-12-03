package com.wolfhouse.influxclient.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * InfluxDB 存储对象字段集合
 *
 * @author Rylin Wolf
 */
public class InfluxFields {
    private final Map<String, Object> fields;

    private InfluxFields(Map<String, Object> fields) {
        this.fields = Collections.synchronizedMap(fields);
    }

    public InfluxFields() {
        fields = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    @SafeVarargs
    public static InfluxFields of(Map.Entry<String, Object>... fields) {
        return new InfluxFields(new LinkedHashMap<>(Map.ofEntries(fields)));
    }

    public static InfluxFields instance() {
        return new InfluxFields();
    }

    public static InfluxFields from(String key, Object value) {
        return InfluxFields.instance().add(key, value);
    }

    public InfluxFields add(String key, Object value) {
        fields.put(key, value);
        return this;
    }

    public InfluxFields addAll(Map<String, Object> fields) {
        this.fields.putAll(fields);
        return this;
    }

    public Object get(String key) {
        return fields.get(key);
    }

    public Object remove(String key) {
        return fields.remove(key);
    }

    public boolean containsKey(String key) {
        return fields.containsKey(key);
    }

    public Set<String> keySet() {
        return fields.keySet();
    }

    public LinkedHashMap<String, Object> toMap() {
        return new LinkedHashMap<>(fields);
    }
}
