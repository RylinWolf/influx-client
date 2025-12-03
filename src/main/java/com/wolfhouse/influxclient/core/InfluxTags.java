package com.wolfhouse.influxclient.core;

import java.util.*;

/**
 * InfluxDB 存储对象标签集合
 *
 * @author Rylin Wolf
 */
public class InfluxTags {

    private final Map<String, String> tags;

    public InfluxTags() {
        tags = Collections.synchronizedMap(new LinkedHashMap<>());
    }

    private InfluxTags(Map<String, String> tags) {
        this.tags = Collections.synchronizedMap(tags);
    }

    @SafeVarargs
    public static InfluxTags of(Map.Entry<String, String>... tags) {
        return new InfluxTags(new LinkedHashMap<>(Map.ofEntries(tags)));
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
        this.tags.putAll(tags);
        return this;
    }

    public Set<String> getTagKeys() {
        return new LinkedHashSet<>(tags.keySet());
    }

    public String getTagValue(String key) {
        return tags.get(key);
    }

    public String removeTag(String key) {
        return tags.remove(key);
    }

    public boolean containsKey(String key) {
        return tags.containsKey(key);
    }

    public Set<String> keySet() {
        return tags.keySet();
    }

    public LinkedHashMap<String, String> toMap() {
        return new LinkedHashMap<>(tags);
    }
}