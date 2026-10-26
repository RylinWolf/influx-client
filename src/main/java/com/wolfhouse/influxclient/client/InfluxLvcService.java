package com.wolfhouse.influxclient.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wolfhouse.influxclient.core.InfluxObjMapper;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.pojo.system.LastCaches;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import com.wolfhouse.influxclient.segment.InfluxBuiltInTableMeta;
import com.wolfhouse.influxclient.segment.from.FromSql;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Last-value-cache
 *
 * @author Rylin Wolf
 */
@Service
@Slf4j
public class InfluxLvcService {
    public static final Integer            HTTP_STATUS_OK      = 200;
    public static final Integer            HTTP_STATUS_CREATED = 201;
    private final       InfluxClient       client;
    private final       HttpClient         httpClient;
    private final       ObjectMapper       objectMapper;
    private final       InfluxDbProperties properties;

    public InfluxLvcService(InfluxDbProperties properties, InfluxClient client) {
        this.httpClient   = HttpClient.newBuilder().build();
        this.objectMapper = new ObjectMapper();
        this.properties   = properties;
        this.client       = client;
    }

    public boolean createLvc(@Nonnull String measurement, @Nonnull Collection<String> keyColumns) {
        return createLvc(measurement, null, keyColumns, null, null, null);
    }

    public boolean createLvc(@Nonnull String measurement, @Nonnull Collection<String> keyColumns, Long ttl) {
        return createLvc(measurement, null, keyColumns, null, null, ttl);
    }

    public boolean createLvc(@Nonnull String measurement, String name, @Nonnull Collection<String> keyColumns, Long ttl) {
        return createLvc(measurement, name, keyColumns, null, null, ttl);
    }

    /**
     * 创建一个 Last-Value-Cache (LVC) 表。
     *
     * @param measurement  测量名称，不能为空。
     * @param name         LVC 表的名称。如果为空，则默认与测量名称相同。
     * @param keyColumns   主键列集合，不能为空。
     * @param valueColumns 缓存字段列集合，可为空。
     * @param count        缓存的最大版本数量，默认为 1。
     * @param ttl          缓存的生命周期（单位为秒），默认为 4 小时。
     * @return 如果创建成功返回 true；否则返回 false。
     * @throws IllegalArgumentException 如果 measurement 或 keyColumns 为空。
     * @throws RuntimeException         如果线程中断。
     */
    public boolean createLvc(@Nonnull String measurement, String name, @Nonnull Collection<String> keyColumns, Collection<String> valueColumns, Integer count, Long ttl) {
        Map<String, Object> body = buildLvcBody(measurement, name, keyColumns, valueColumns, count, ttl);
        try {
            HttpRequest request = requestBuilder()
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(body),
                                                              StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() != HTTP_STATUS_CREATED) {
                throw new IOException("创建 LVC 失败, 意外的状态码: " + resp.statusCode() + "\n resp: " + resp.body());
            }
            return true;
        } catch (IOException e) {
            log.error("[InfluxClient] 创建 LVC 失败", e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public boolean deleteLvc(String measurement, String name) {
        String uri = String.format("%s/api/v3/configure/last_cache?db=%s&table=%s&name=%s",
                                   properties.getUrl(),
                                   properties.getDatabase(),
                                   measurement,
                                   name);
        HttpRequest request = requestBuilder()
                .DELETE()
                .uri(URI.create(uri))
                .build();
        try {
            HttpResponse<String> resp = httpClient.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() != HTTP_STATUS_OK) {
                throw new IOException("删除 LVC 失败, 意外的状态码: " + resp.statusCode() + "\n resp: " + resp.body());
            }
        } catch (IOException e) {
            log.error("[InfluxClient] 删除 LVC 失败", e);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * 获取所有 LVC 表
     *
     * @return List<{@link LastCaches}> LVC 表集合
     */
    public List<LastCaches> lvcTables() {
        return InfluxObjMapper.mapAll(
                client.query(InfluxQueryWrapper.system(FromSql.systemLastCache())
                                               .selectAll()
                                               .withTime(false)),
                LastCaches.class,
                InfluxBuiltInTableMeta.TABLE_LAST_CACHE_COLUMNS);
    }

    private HttpRequest.Builder requestBuilder() {
        return HttpRequest.newBuilder()
                          .uri(URI.create(properties.getUrl() + "/api/v3/configure/last_cache"))
                          .header("Authorization", "Bearer " + properties.getToken())
                          .header("Content-Type", "application/json")
                          .timeout(properties.getLvcTimeout());
    }

    private Map<String, Object> buildLvcBody(String measurement, String name, Collection<String> keyColumns, Collection<String> valueColumns, Integer count, Long ttl) {
        Map<String, Object> map = HashMap.newHashMap(6);
        // 表、标签不得为空
        Assert.hasText(measurement, "measurement 不得为空");
        if (CollectionUtils.isEmpty(keyColumns)) {
            throw new IllegalArgumentException("keyColumns 不能为空");
        }
        // 初始化 lvc 表名
        name = StringUtils.hasText(name) ? name : measurement;
        map.put("db", properties.getDatabase());
        map.put("table", measurement);
        map.put("name", name);
        map.put("key_columns", keyColumns);
        // 缓存字段列允许为空
        if (!CollectionUtils.isEmpty(valueColumns)) {
            map.put("value_columns", valueColumns);
        }
        // count 默认为 1
        count = count == null ? 1 : Math.clamp(count, 1, Integer.MAX_VALUE);
        map.put("count", count);
        // ttl 默认为 4 小时（等同于不传递时的默认值）
        ttl = ttl == null || ttl < 0 ? 4 * 3600 : Math.clamp(ttl, ttl, Long.MAX_VALUE);
        map.put("ttl", ttl);
        return map;
    }
}
