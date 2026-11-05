package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.segment.select.AggSql;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 验证 LVC 查询转换后的父引用、参数和可变查询状态相互隔离。
 *
 * @author Rylin Wolf
 */
class InfluxLvcQueryIsolationTest {
    /** 原始测量表名 */
    private static final String MEASUREMENT = "sensor";
    /** 测试使用的缓存名称 */
    private static final String CACHE_NAME = "sensor_cache";
    /** 预期使用的 LVC 来源 */
    private static final String CACHE_SOURCE = "last_cache('sensor', 'sensor_cache')";

    /** 验证已有条件和修饰符的链式构建使用 LVC，并独立维护查询状态。 */
    @Test
    void copiedConditionsAndModifiersBuildAgainstLvc() {
        InfluxClient client = new InfluxClient(mock(InfluxDBClient.class));
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).withTime(false)
                .select("id").selectO(AggSql.max("value").as("maximum"));
        source.where().eq("tenant_id", "tenant-a");
        source.modify().setGlobalDesc(false);
        source.modify().groupBy("id").orderBy("id").limit(10, 2);
        String originalSql = source.build();

        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(source, CACHE_NAME);

        // 1. 条件和修饰符属于新查询器，同时保留原有分页与排序设置
        assertNotSame(source.where(), cached.where());
        assertNotSame(source.modify(), cached.modify());
        assertSame(cached, cached.where().parent());
        assertSame(cached, cached.modify().getParent());
        assertEquals(10L, cached.modify().getLimit());
        assertEquals(2L, cached.modify().getOffset());
        assertEquals(Boolean.FALSE, cached.modify().getGlobalDesc());
        assertTrue(cached.where().build().contains(CACHE_SOURCE));

        // 2. 新查询追加筛选、分组和排序后，原查询仍保持原有 SQL
        String cachedSql = cached.where().eq("site", "site-a").build();
        assertTrue(cachedSql.contains(CACHE_SOURCE));
        cached.modify().groupBy("site").orderBy("site").limit(1, 0);
        assertTrue(cached.modify().build().contains(CACHE_SOURCE));
        assertEquals(originalSql, source.build());
        assertEquals(List.of("id"), List.copyOf(source.modify().getGroupBy()));
        assertEquals(List.of("id"), List.copyOf(source.modify().getOrderBy()));
        assertEquals(Map.of("param_1", "tenant-a"), source.where().getParameters());
    }

    /** 验证转换前没有 WHERE 时，后续 HAVING 参数仍会绑定到 LVC 查询。 */
    @Test
    void havingAddedAfterConversionBindsParametersToLvcQuery() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            assertTrue(sql.contains(CACHE_SOURCE));
            assertTrue(sql.contains("HAVING"));
            assertFalse(sql.contains("WHERE"));
            assertEquals(Map.of("param_1", 1L), invocation.getArgument(1));
            return Stream.empty();
        });
        InfluxClient client = new InfluxClient(driver);
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).withTime(false)
                .select("id").selectO(AggSql.max("value").as("maximum"));
        source.modify().groupBy("id");

        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(source, CACHE_NAME);
        cached.modify().having(condition -> condition.gt("maximum", 1L));
        try (Stream<Object[]> results = client.query(cached, false)) {
            assertEquals(0L, results.count());
        }

        assertNull(source.getConditionWrapper());
        assertNull(source.modify().getHaving());
        verify(driver).query(anyString(), anyMap());
        verify(driver, never()).query(anyString());
    }

    /** 验证已有 HAVING 和新增条件共享副本序号，不覆盖原有参数。 */
    @Test
    void copiedHavingPreservesParametersAndAllocatesNewNamesIndependently() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            Map<String, Object> expected = Map.of("param_1", "tenant-a", "param_2", 1L,
                    "param_3", "site-a", "param_4", 2L);
            assertTrue(sql.contains(CACHE_SOURCE));
            assertTrue(sql.contains("$param_2"));
            assertTrue(sql.contains("$param_4"));
            assertEquals(expected, invocation.getArgument(1));
            return Stream.empty();
        });
        InfluxClient client = new InfluxClient(driver);
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).withTime(false)
                .select("id").selectO(AggSql.max("value").as("maximum"));
        source.where().eq("tenant_id", "tenant-a");
        source.modify().groupBy("id").having(condition -> condition.gt("maximum", 1L));
        String originalHaving = source.modify().getHaving();

        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(source, CACHE_NAME);
        cached.where().eq("site", "site-a");
        cached.modify().having(condition -> condition.lt("maximum", 2L));
        try (Stream<Object[]> results = client.query(cached, false)) {
            assertEquals(0L, results.count());
        }

        assertEquals(Map.of("param_1", "tenant-a", "param_2", 1L), source.where().getParameters());
        assertEquals(originalHaving, source.modify().getHaving());
        source.where().eq("source_only", "original");
        assertEquals("original", source.where().getParameters().get("param_3"));
        assertEquals("site-a", cached.where().getParameters().get("param_3"));
    }

    /** 验证普通选列和聚合选列均独立复制，并保留原有顺序。 */
    @Test
    void selectionMapsAreIsolatedInBothDirections() {
        InfluxClient client = new InfluxClient(mock(InfluxDBClient.class));
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).withTime(false)
                .select("id as device").selectO(AggSql.max("value").as("maximum"));

        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(source, CACHE_NAME);
        cached.select("humidity as moisture").selectO(AggSql.min("value").as("minimum"));
        source.select("temperature as temp").selectO(AggSql.max("humidity").as("high"));

        assertEquals(List.of("device", "temp"), List.copyOf(source.getAliasMap().values()));
        assertEquals(List.of("device", "moisture"), List.copyOf(cached.getAliasMap().values()));
        assertEquals(List.of("maximum", "high"), List.copyOf(source.getFuncAliasMap().values()));
        assertEquals(List.of("maximum", "minimum"), List.copyOf(cached.getFuncAliasMap().values()));
        assertTrue(cached.build().contains(CACHE_SOURCE));
    }

    /** 验证原查询尚未创建条件或修饰符时，转换后的新建对象也归属于 LVC。 */
    @Test
    void lazilyCreatedConditionsAndModifiersBelongToLvc() {
        InfluxClient client = new InfluxClient(mock(InfluxDBClient.class));
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).select("value");

        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(source, CACHE_NAME);
        assertTrue(cached.where().eq("id", "device-1").build().contains(CACHE_SOURCE));
        assertTrue(cached.modify().orderBy("time").limit(1).build().contains(CACHE_SOURCE));

        assertNull(source.getConditionWrapper());
        assertNull(source.getModifiersWrapper());
    }
}
