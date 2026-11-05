package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxResult;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 验证最近记录查询的空结果处理和缓存失败任务跟踪。
 *
 * @author Rylin Wolf
 */
class InfluxClientRegressionTest {
    /** 测试使用的测量表 */
    private static final String MEASUREMENT = "sensor";
    /** 异步回调完成的最大等待时间 */
    private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(5);
    /** 确认失败任务持续保留的观察时间 */
    private static final Duration OBSERVATION_DURATION = Duration.ofMillis(200);
    /** 测试期间避免触发周期性刷写的时间间隔 */
    private static final Duration FLUSH_INTERVAL = Duration.ofHours(1);

    /** 验证无匹配分组时，各种公开查询入口都返回空内容。 */
    @Test
    void recentWithoutMatchesReturnsEmptyAcrossQueryEntries() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation -> Stream.empty());
        InfluxClient client = new InfluxClient(driver);
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).select("v");
        source.where().eq("id", "missing");

        InfluxQueryWrapper<?> recent = client.addQueryRecent(source, true, "id");
        assertTrue(recent.isEmptyResult());
        assertTrue(recent.build().contains("WHERE FALSE"));
        verify(driver).query(anyString(), anyMap());
        clearInvocations(driver);

        // 验证计数、映射、分页和原始流入口均直接返回空内容
        assertEquals(0L, client.count(recent));
        try (Stream<Object[]> checked = client.query(recent);
             Stream<Object[]> unchecked = client.query(recent, false)) {
            assertEquals(0L, checked.count());
            assertEquals(0L, unchecked.count());
        }
        assertTrue(client.queryMap(recent).isEmpty());
        assertTrue(client.queryMap(recent, false).isEmpty());
        assertTrue(client.queryMap(recent, InfluxResult.class).isEmpty());
        assertTrue(client.queryResult(recent).rows().isEmpty());
        assertTrue(client.pagination(recent, InfluxResult.class, 1, 10).records().isEmpty());

        // 转换到 LVC 来源后仍保留已确定的空结果语义
        InfluxQueryWrapper<?> cached = client.addQueryRecentByCache(recent, "sensor_cache");
        assertTrue(cached.build().contains("WHERE FALSE"));
        assertTrue(client.queryMap(cached, false).isEmpty());
        InfluxQueryWrapper<?> repeated = client.addQueryRecent(recent, true, "id");
        assertTrue(repeated.isEmptyResult());
        assertTrue(client.queryMap(repeated, false).isEmpty());
        verifyNoInteractions(driver);
    }

    /** 验证存在匹配分组时，仍可正常构造并执行回表查询。 */
    @Test
    void recentWithMatchesStillQueriesMatchingRows() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.contains("information_schema.columns")) {
                return Stream.<Object[]>of(new Object[]{"id"}, new Object[]{"time"}, new Object[]{"v"});
            }
            if (sql.contains("max(")) {
                return Stream.<Object[]>of(new Object[]{"matched", "2026-11-05T00:00:00Z"});
            }
            return Stream.<Object[]>of(new Object[]{"matched", "2026-11-05T00:00:00Z", 1L});
        });
        InfluxClient client = new InfluxClient(driver);
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).select("v");
        source.where().eq("id", "matched");

        InfluxQueryWrapper<?> recent = client.addQueryRecent(source, true, "id");
        assertFalse(recent.isEmptyResult());
        List<Map<String, Object>> rows = client.queryMap(recent, false);
        assertEquals(1, rows.size());
        assertEquals("matched", rows.getFirst().get("id"));
        assertEquals(1L, rows.getFirst().get("v"));
    }

    /** 验证回表保留原筛选、原 OR 分支优先级和独立的参数序号。 */
    @Test
    void recentRetainsOriginalFilterAsAnIndependentConditionGroup() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicReference<Map<String, Object>> groupedParameters = new AtomicReference<>();
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.contains("information_schema.columns")) {
                return Stream.<Object[]>of(new Object[]{"id"}, new Object[]{"time"}, new Object[]{"tenant_id"});
            }
            groupedParameters.set(Map.copyOf(invocation.getArgument(1)));
            return Stream.<Object[]>of(new Object[]{"device-1", "2026-11-05T00:00:00Z"},
                    new Object[]{"device-2", "2026-11-05T01:00:00Z"});
        });
        InfluxClient client = new InfluxClient(driver);
        InfluxQueryWrapper<?> source = InfluxQueryWrapper.create(MEASUREMENT).select("id");
        source.where().eq("tenant_id", "tenant-a")
                .or(condition -> condition.eq("tenant_id", "tenant-b"));
        String originalSql = source.where().sql();
        Map<String, Object> originalParameters = Map.copyOf(source.where().getParameters());

        InfluxQueryWrapper<?> recent = client.addQueryRecent(source, true, "id");

        // 1. 回表的原筛选被括号包裹，完整保留 OR 分支的优先级
        String expectedPrefix = "( " + originalSql + " ) AND (";
        assertTrue(recent.where().sql().startsWith(expectedPrefix));
        assertEquals("tenant-a", recent.where().getParameters().get("param_1"));
        assertEquals("tenant-b", recent.where().getParameters().get("param_2"));
        assertTrue(recent.where().getParameters().containsValue("device-1"));
        assertTrue(recent.where().getParameters().containsValue("device-2"));
        assertEquals(originalParameters, groupedParameters.get());

        // 2. 回表追加参数不会改变原查询，再次使用原查询仍从原序号继续
        assertNotSame(source.where(), recent.where());
        assertSame(recent, recent.where().parent());
        assertEquals(originalSql, source.where().sql());
        assertEquals(originalParameters, source.where().getParameters());
        source.where().eq("source_only", "kept");
        assertEquals("kept", source.where().getParameters().get("param_3"));
        assertFalse(recent.where().getParameters().containsValue("kept"));
    }

    /** 验证原查询没有条件时，仍能按匹配到的最近记录构造回表条件。 */
    @Test
    void recentWithoutOriginalFilterStillRestrictsMatchedRecords() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        when(driver.query(anyString())).thenAnswer(invocation ->
                Stream.<Object[]>of(new Object[]{"device-1", "2026-11-05T00:00:00Z"}));
        when(driver.query(anyString(), anyMap())).thenAnswer(invocation ->
                Stream.<Object[]>of(new Object[]{"id"}, new Object[]{"time"}));
        InfluxClient client = new InfluxClient(driver);

        InfluxQueryWrapper<?> recent = client.addQueryRecent(InfluxQueryWrapper.create(MEASUREMENT), true, "id");

        assertFalse(recent.isEmptyResult());
        assertTrue(recent.build().contains("WHERE"));
        assertTrue(recent.where().getParameters().containsValue("device-1"));
    }

    /** 验证失败任务在后续成功刷写后仍保留在跟踪队列中。 */
    @Test
    void failedCacheTaskRemainsTrackedAfterLaterSuccessfulFlush() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        doThrow(new IllegalStateException("模拟缓存写入失败"))
                .doNothing()
                .when(driver).writePoints(anyList());
        InfluxClient client = new InfluxClient(driver);
        client.setCacheFlushInterval(FLUSH_INTERVAL);
        try {
            // 1. 触发失败写入，等待完成回调并确认任务持续保留
            AbstractActionInfluxObj point = new AbstractActionInfluxObj(MEASUREMENT) {};
            point.addTag("id", "audit").addField("v", 1L);
            client.insertCache(point);
            CompletableFuture<Void> failed = client.handleCache();
            assertThrows(CompletionException.class, failed::join);
            await().during(OBSERVATION_DURATION).atMost(WAIT_TIMEOUT)
                    .untilAsserted(() -> assertTrue(client.insertTasks.contains(failed)));
            assertFalse(client.isInsertTaskAllDone());

            // 2. 后续成功任务正常退出，原失败任务继续保留
            client.insertCache(point);
            client.handleCache().join();
            await().atMost(WAIT_TIMEOUT).untilAsserted(() -> {
                assertEquals(List.of(failed), List.copyOf(client.insertTasks));
                assertFalse(client.isInsertTaskAllDone());
            });
            verify(driver, times(2)).writePoints(anyList());
        } finally {
            client.close();
        }
    }

    /** 验证全部写入成功时，跟踪队列正常清空。 */
    @Test
    void successfulCacheTaskIsRemoved() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        InfluxClient client = new InfluxClient(driver);
        client.setCacheFlushInterval(FLUSH_INTERVAL);
        try {
            AbstractActionInfluxObj point = new AbstractActionInfluxObj(MEASUREMENT) {};
            point.addTag("id", "audit").addField("v", 1L);
            client.insertCache(point);
            client.handleCache().join();

            await().atMost(WAIT_TIMEOUT).until(client::isInsertTaskAllDone);
            verify(driver).writePoints(anyList());
        } finally {
            client.close();
        }
    }
}
