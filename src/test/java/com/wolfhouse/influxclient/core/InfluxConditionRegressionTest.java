package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.segment.select.AggSql;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证动态嵌套条件和独立 HAVING 子句的构造。
 *
 * @author Rylin Wolf
 */
class InfluxConditionRegressionTest {
    /** 验证未启用的首个 AND 条件既不执行回调，也不保留空括号。 */
    @Test
    void disabledFirstAndDoesNotLeaveEmptyGroup() {
        InfluxQueryWrapper<?> wrapper = InfluxQueryWrapper.create("sensor").select("value");
        AtomicInteger calls = new AtomicInteger();
        wrapper.where().and(condition -> {
            calls.incrementAndGet();
            condition.eq("id", "skipped");
        }, false).eq("id", "kept");

        assertEquals(0, calls.get());
        assertEquals(Map.of("param_1", "kept"), wrapper.where().getParameters());
        assertEquals("SELECT `value`,time FROM `sensor` WHERE ( ( `id` = $param_1 ) )", wrapper.build());
    }

    /** 验证未启用的首个 OR 条件不会污染后续基本条件。 */
    @Test
    void disabledFirstOrDoesNotLeaveEmptyGroup() {
        InfluxQueryWrapper<?> wrapper = InfluxQueryWrapper.create("sensor").select("value");
        wrapper.where().or(condition -> fail("未启用的条件不应执行回调"), false).eq("id", "kept");

        assertEquals(Map.of("param_1", "kept"), wrapper.where().getParameters());
        assertEquals("SELECT `value`,time FROM `sensor` WHERE ( ( `id` = $param_1 ) )", wrapper.build());
    }

    /** 验证回调中的所有子条件被跳过时，也不会生成空条件组。 */
    @Test
    void emptyNestedConditionsAreSkippedBeforeAndAfterExistingConditions() {
        InfluxQueryWrapper<?> wrapper = InfluxQueryWrapper.create("sensor").select("value");
        wrapper.where().and(condition -> condition.eq("id", "skipped", false))
                .or(condition -> condition.eq("id", "skipped", false))
                .eq("id", "kept")
                .and(condition -> condition.eq("id", "skipped", false))
                .or(condition -> condition.eq("id", "skipped", false));

        assertEquals(Map.of("param_1", "kept"), wrapper.where().getParameters());
        assertEquals("SELECT `value`,time FROM `sensor` WHERE ( ( `id` = $param_1 ) )", wrapper.build());
    }

    /** 验证没有 WHERE 的聚合过滤只生成 HAVING 子句。 */
    @Test
    void havingWithoutWhereDoesNotGenerateEmptyWhere() {
        InfluxQueryWrapper<?> wrapper = InfluxQueryWrapper.create("sensor")
                .withTime(false).select("id").selectO(AggSql.max("value").as("maximum"));
        wrapper.modify().groupBy("id").having(condition -> condition.gt("maximum", 1L));

        String sql = wrapper.build();
        assertFalse(sql.contains("WHERE"));
        assertTrue(sql.contains("HAVING"));
        assertEquals(Map.of("param_1", 1L), wrapper.where().getParameters());
    }

    /** 验证带字段约束的查询可同时使用 WHERE 和引用聚合别名的 HAVING。 */
    @Test
    void whereAndHavingShareParametersWithoutValidatingAliasAsWhereColumn() {
        AbstractActionInfluxObj schema = new AbstractActionInfluxObj("sensor") {};
        schema.addTag("id", null).addField("value", null);
        InfluxQueryWrapper<?> wrapper = InfluxQueryWrapper.from(schema)
                .withTime(false).select("id").selectO(AggSql.max("value").as("maximum"));
        wrapper.where().eq("id", "matched");
        wrapper.modify().groupBy("id").having(condition -> condition.gt("maximum", 1L));

        String sql = assertDoesNotThrow(wrapper::build);
        assertTrue(sql.contains("WHERE"));
        assertTrue(sql.contains("HAVING"));
        assertEquals(Map.of("param_1", "matched", "param_2", 1L), wrapper.where().getParameters());
    }
}
