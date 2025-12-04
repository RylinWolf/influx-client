package com.wolfhouse.influxclient.core;

import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * InfluxDB 存储对象标签集合
 *
 * @author Rylin Wolf
 */
@Slf4j
public class InfluxTags {

    private final LinkedHashMap<String, String> tags;

    public InfluxTags() {
        tags = new LinkedHashMap<>();
    }

    private InfluxTags(Map<String, String> tags) {
        this.tags = new LinkedHashMap<>(tags);
    }

    @SafeVarargs
    public static InfluxTags of(Map.Entry<String, String>... tags) {
        return new InfluxTags(new LinkedHashMap<>(Map.ofEntries(tags)));
    }

    public static InfluxTags of(Map<String, String> fields) {
        return new InfluxTags(fields);
    }

    public static InfluxTags instance() {
        return new InfluxTags();
    }

    public static InfluxTags from(String key, String value) {
        return InfluxTags.instance().add(key, value);
    }

    public InfluxTags add(String key, String value) {
        tags.put(key, value);
        return this;
    }

    public InfluxTags addAll(Map<String, String> tags) {
        assert tags != null : "【InfluxTags】要填加的标签不得为 null";
        if (tags.isEmpty()) {
            return this;
        }
        if (!(LinkedHashMap.class.isAssignableFrom(tags.getClass()))) {
            log.warn("【InfluxTags】要添加的标签是无序的。若是查询操作，应使用有序 Map，否则可能导致结果集映射出错！");
        }
        // LinkedHashMap 重写了 forEach，因此无需特殊区分
        tags.forEach(this::add);
        return this;
    }

    public Set<String> getTagKeys() {
        return new LinkedHashSet<>(tags.keySet());
    }

    public String getTagValue(String key) {
        return tags.get(key);
    }

    public Map.Entry<String, String> getFirst() {
        return tags.firstEntry();
    }

    public boolean isEmpty() {
        return tags.isEmpty();
    }

    public String remove(String key) {
        return tags.remove(key);
    }

    public boolean containsKey(String key) {
        return tags.containsKey(key);
    }
    
    public LinkedHashMap<String, String> toMap() {
        return new LinkedHashMap<>(tags);
    }
}