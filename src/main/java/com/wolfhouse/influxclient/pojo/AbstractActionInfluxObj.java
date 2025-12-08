package com.wolfhouse.influxclient.pojo;

import com.wolfhouse.influxclient.exception.DuplicateFieldTagException;
import lombok.Getter;
import lombok.ToString;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * InfluxDB数据映射对象的抽象基类。
 * 该类提供了向 InfluxDB 写入、查询数据所需的基本结构和方法。
 * 子类继承后，应当重写 {@link AbstractActionInfluxObj#tableName()} ()}方法，或通过 `super.measurement` 设置该超类的 measurement 字段值。
 * <p>
 * 该类在添加标签、字段时会进行交叉检查，确保标签和字段没有交集。
 *
 * @author Rylin Wolf
 */
@Getter
@ToString(callSuper = true)
@SuppressWarnings({"UnusedReturnValue", "unused"})
public abstract class AbstractActionInfluxObj extends AbstractBaseInfluxObj {
    /** InfluxDB的标签集合，用于存储标签数据 */
    protected InfluxTags   tags;
    /** InfluxDB的字段集合，用于存储字段数据 */
    protected InfluxFields fields;

    protected AbstractActionInfluxObj() {
        super();
    }

    /**
     * 向当前对象中添加 InfluxDB 的标签数据。
     *
     * @param tags 要添加的标签集合，包含多个键值对形式的标签，用于标识数据的维度信息。
     */
    public final void addTags(InfluxTags tags) {
        assert tags != null : "标签对象不得为 null";
        this.addTag(tags.toMap());
    }

    /**
     * 向当前对象中添加 InfluxDB 的字段数据。
     *
     * @param fields 要添加的字段集合，包含多个键值对形式的字段，用于存储数据的具体值和内容。
     */
    public final void addFields(InfluxFields fields) {
        assert fields != null : "字段对象不得为 null";
        this.addField(fields.toMap());
    }

    /**
     * 向当前对象中添加一个标签键值对。如果标签集合尚未初始化，则创建新的标签集合并添加键值对。
     * 如果标签集合已存在，则直接添加新的键值对到集合中。
     *
     * @param key   标签的键名，表示标签的识别符，用于标识标签名称。
     * @param value 标签的值，对应指定键的具体值，存储标签的内容。
     * @return 当前对象的标签集合，包含新的标签数据。
     */
    public final InfluxTags addTag(String key, String value) {
        // 检查标签中是否包含该字段
        if (this.fields != null && this.fields.containsKey(key)) {
            throw new DuplicateFieldTagException(key);
        }
        // 若标签未初始化，则初始化标签对象
        if (this.tags == null) {
            this.tags = InfluxTags.from(key, value);
            return this.tags;
        }
        this.tags.add(key, value);
        return this.tags;
    }

    /**
     * 向当前对象中添加一组标签键值对。如果当前标签集合尚未初始化，则创建新的标签集合并添加所有给定的标签。
     * 如果标签集合已存在，则将给定标签集合中的所有标签添加到当前集合中。
     *
     * @param tags 要添加的标签集合，包含多个键值对形式的标签，用于标识数据的维度信息。
     * @return 添加完成后的标签集合。
     */
    public final InfluxTags addTag(Map<String, String> tags) {
        // 字段中是否有重复键
        if (this.fields != null) {
            Set<String> fieldKeys = this.fields.getFieldKeys();
            fieldKeys.retainAll(tags.keySet());
            // 保留重复值，若不为空则说明有重复
            if (!fieldKeys.isEmpty()) {
                throw new DuplicateFieldTagException(fieldKeys.toArray(new String[0]));
            }
        }
        if (this.tags == null) {
            this.tags = InfluxTags.of(tags);
            return this.tags;
        }
        return this.tags.addAll(tags);
    }

    /**
     * 向当前对象中添加一个字段键值对。如果字段集合尚未初始化，则创建新的字段集合并添加键值对。
     * 如果字段集合已存在，则直接添加新的键值对到集合中。
     *
     * @param key   字段的键名，表示字段识别符，用于标识字段名称。
     * @param value 字段的值，对应指定键的具体值，存储字段的内容。
     * @return 当前对象的字段集合，包含新的字段数据。
     */
    public final InfluxFields addField(String key, Object value) {
        // 检查标签中是否包含该字段
        if (this.tags != null && this.tags.containsKey(key)) {
            throw new DuplicateFieldTagException(key);
        }
        // 若字段未初始化，则初始化字段对象
        if (this.fields == null) {
            this.fields = InfluxFields.from(key, value);
            return this.fields;
        }
        this.fields.add(key, value);
        return this.fields;
    }

    /**
     * 向当前对象中添加 InfluxDB 的字段数据。如果字段集合尚未初始化，则创建新的字段集合并添加所有的字段。
     * 如果字段集合已存在，则直接将给定字段集合中所有字段添加到当前字段集合中。
     *
     * @param fields 要添加的字段集合，包含多个键值对形式的字段，用于存储数据的具体值和内容。
     * @return 当前对象的字段集合，包含新的字段数据。
     */
    public final InfluxFields addField(Map<String, Object> fields) {
        // 标签中是否有重复键
        if (this.tags != null) {
            Set<String> tagKeys = this.tags.getTagKeys();
            tagKeys.retainAll(fields.keySet());
            // 保留重复值，若不为空则说明有重复
            if (!tagKeys.isEmpty()) {
                throw new DuplicateFieldTagException(tagKeys.toArray(new String[0]));
            }
        }
        if (this.fields == null) {
            this.fields = InfluxFields.of(fields);
            return this.fields;
        }
        return this.fields.addAll(fields);
    }

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
        return tags.getTagKeys();
    }

    /**
     * 获取所有字段的键名集合
     *
     * @return 字段键名集合
     */
    public Set<String> getFieldKeys() {
        return fields.getFieldKeys();
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
