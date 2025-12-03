package com.wolfhouse.influxclient.core;

import lombok.Data;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * @author Rylin Wolf
 */
@Data
public abstract class AbstractInsertObj {
    protected String       measurement;
    protected Instant      timestamp;
    protected InfluxTags   tags;
    protected InfluxFields fields;

    protected AbstractInsertObj(String measurement) {
        this.timestamp   = Instant.now();
        this.measurement = measurement;
    }

    protected AbstractInsertObj() {
        this(null);
    }

    /**
     * 向当前对象中添加 InfluxDB 的标签数据。
     *
     * @param tags 要添加的标签集合，包含多个键值对形式的标签，用于标识数据的维度信息。
     */
    public void addTags(InfluxTags tags) {
        if (this.tags == null) {
            this.tags = tags;
            return;
        }
        this.tags.addAll(tags.toMap());
    }

    /**
     * 向当前对象中添加 InfluxDB 的字段数据。
     *
     * @param fields 要添加的字段集合，包含多个键值对形式的字段，用于存储数据的具体值和内容。
     */
    public void addFields(InfluxFields fields) {
        if (this.fields == null) {
            this.fields = fields;
            return;
        }
        this.fields.addAll(fields.toMap());
    }

    /**
     * 获取 InfluxDB 对象对应的表名。
     *
     * @return 表名
     */
    public abstract String getMeasurement();

    public boolean isTag(String name) {
        return tags.containsKey(name);
    }

    public boolean isField(String name) {
        return fields.containsKey(name);
    }

    public Set<String> getTagKeys() {
        return tags.keySet();
    }

    public Set<String> getFieldKeys() {
        return fields.keySet();
    }

    public LinkedHashMap<String, Object> getFieldMap() {
        return fields.toMap();
    }

    public LinkedHashMap<String, String> getTagMap() {
        return tags.toMap();
    }


}
