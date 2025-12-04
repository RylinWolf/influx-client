package com.wolfhouse.influxclient.core;

import lombok.Data;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * InfluxDB数据插入对象的抽象基类。
 * 该类提供了向InfluxDB写入数据所需的基本结构和方法。
 * 子类继承后，应当提供 measurement 字段值。
 *
 * @author Rylin Wolf
 */
@Data
public abstract class AbstractInsertObj {
    /** InfluxDB的度量名称，用于指定数据写入的表 */
    protected final String       measurement;
    /** 数据点的时间戳 */
    protected       Instant      timestamp;
    /** InfluxDB的标签集合，用于存储标签数据 */
    protected       InfluxTags   tags;
    /** InfluxDB的字段集合，用于存储字段数据 */
    protected       InfluxFields fields;

    /**
     * 默认构造函数，度量名称为null
     */
    protected AbstractInsertObj() {
        this.timestamp   = Instant.now();
        this.measurement = tableName();
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
     * 设置 InfluxDB 对象对应的表名
     *
     * @return 表名
     */
    protected abstract String tableName();

    /**
     * 检查指定的名称是否为标签
     *
     * @param name 要检查的名称
     * @return 如果是标签返回true，否则返回false
     */
    public boolean isTag(String name) {
        return tags.containsKey(name);
    }

    /**
     * 检查指定的名称是否为字段
     *
     * @param name 要检查的名称
     * @return 如果是字段返回true，否则返回false
     */
    public boolean isField(String name) {
        return fields.containsKey(name);
    }

    /**
     * 获取所有标签的键名集合
     *
     * @return 标签键名集合
     */
    public Set<String> getTagKeys() {
        return tags.keySet();
    }

    /**
     * 获取所有字段的键名集合
     *
     * @return 字段键名集合
     */
    public Set<String> getFieldKeys() {
        return fields.keySet();
    }

    /**
     * 获取字段的键值对映射
     *
     * @return 包含所有字段的LinkedHashMap
     */
    public LinkedHashMap<String, Object> getFieldMap() {
        return fields.toMap();
    }

    /**
     * 获取标签的键值对映射
     *
     * @return 包含所有标签的LinkedHashMap
     */
    public LinkedHashMap<String, String> getTagMap() {
        return tags.toMap();
    }


}
