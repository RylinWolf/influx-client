package com.wolfhouse.influxclient.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @author Rylin Wolf
 */
@ConfigurationProperties(prefix = "influx")
@Data
public class InfluxDbProperties {
    /**
     * InfluxDB 访问令牌（可从环境变量 INFLUX_TOKEN 读取）。
     */
    private String token = System.getenv("INFLUX_TOKEN");

    /**
     * InfluxDB 服务器地址，例如：http://localhost:8086
     */
    private String url;

    /**
     * 目标数据库（Bucket）。
     */
    private String database;

    /** 缓存区数量上限, 默认 1000 条 */
    private Long cacheBound = 1000L;

    /** 缓存区刷新时间间隔(ms), 默认 1 分钟 */
    private Long cacheFlushInterval = Duration.ofMinutes(1).toMillis();
}
