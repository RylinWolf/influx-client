package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.anno.OverrideColumn;
import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证默认值保护、覆盖注解和空值覆盖规则。
 *
 * @author Rylin Wolf
 */
class InfluxObjMapperOverrideTest {
    /** 验证没有覆盖注解时保留引用类型和基本类型的已有初值。 */
    @Test
    void unannotatedDefaultsArePreserved() {
        DefaultRow row = InfluxObjMapper.map(new Object[]{"incoming", 9}, DefaultRow.class, List.of("value", "count"));

        assertEquals("initial", row.value);
        assertEquals(7, row.count);
    }

    /** 验证没有初值的普通字段仍可正常接收查询结果。 */
    @Test
    void uninitializedFieldIsPopulated() {
        EmptyRow row = InfluxObjMapper.map(new Object[]{"incoming"}, EmptyRow.class, List.of("value"));

        assertEquals("incoming", row.value);
    }

    /** 验证字段级注解允许覆盖已有初值。 */
    @Test
    void fieldAnnotationAllowsOverwrite() {
        FieldOverrideRow row = InfluxObjMapper.map(new Object[]{"incoming"}, FieldOverrideRow.class, List.of("value"));

        assertEquals("incoming", row.value);
    }

    /** 验证覆盖注解默认忽略空查询值。 */
    @Test
    void defaultOverrideIgnoresNull() {
        FieldOverrideRow row = InfluxObjMapper.map(new Object[]{null}, FieldOverrideRow.class, List.of("value"));

        assertEquals("initial", row.value);
    }

    /** 验证类级注解允许覆盖。 */
    @Test
    void classAnnotationAllowsOverwrite() {
        ClassOverrideRow row = InfluxObjMapper.map(new Object[]{"incoming"}, ClassOverrideRow.class, List.of("value"));

        assertEquals("incoming", row.value);
    }

    /** 验证字段级空值策略优先于类级策略。 */
    @Test
    void fieldAnnotationCanAllowNullDespiteClassDefault() {
        NullOverrideRow row = InfluxObjMapper.map(new Object[]{null}, NullOverrideRow.class, List.of("value"));

        assertNull(row.value);
    }

    /**
     * 含有普通字段初值的结果对象。
     *
     * @author Rylin Wolf
     */
    public static class DefaultRow extends AbstractBaseInfluxObj {
        /** 文本字段初值 */
        public String value = "initial";
        /** 基本类型字段初值 */
        public int count = 7;
    }

    /**
     * 未初始化的结果对象。
     *
     * @author Rylin Wolf
     */
    public static class EmptyRow extends AbstractBaseInfluxObj {
        /** 等待映射的文本字段 */
        public String value;
    }

    /**
     * 允许字段覆盖的结果对象。
     *
     * @author Rylin Wolf
     */
    public static class FieldOverrideRow extends AbstractBaseInfluxObj {
        /** 允许覆盖的文本字段 */
        @OverrideColumn
        public String value = "initial";
    }

    /**
     * 允许类级覆盖的结果对象。
     *
     * @author Rylin Wolf
     */
    @OverrideColumn
    public static class ClassOverrideRow extends AbstractBaseInfluxObj {
        /** 使用类级覆盖策略的文本字段 */
        public String value = "initial";
    }

    /**
     * 字段级策略覆盖类级策略的结果对象。
     *
     * @author Rylin Wolf
     */
    @OverrideColumn
    public static class NullOverrideRow extends AbstractBaseInfluxObj {
        /** 明确允许空值覆盖的文本字段 */
        @OverrideColumn(ignoreNull = false)
        public String value = "initial";
    }
}
