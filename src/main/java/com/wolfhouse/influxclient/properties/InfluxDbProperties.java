package com.wolfhouse.influxclient.properties;

import lombok.Data;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * @author Rylin Wolf
 */
@ConfigurationProperties(prefix = "influx")
@Data
public class InfluxDbProperties {
    /** 日志令牌首尾各自保留的字符数 */
    private static final int TOKEN_VISIBLE_LENGTH = 4;
    /** 日志令牌被隐藏部分的占位文本 */
    private static final String TOKEN_MASK = "****";

    /**
     * InfluxDB 访问令牌（可从环境变量 INFLUX_TOKEN 读取）。
     */
    @ToString.Exclude
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

    /** LVC HTTP 连接超时时间 */
    private Duration lvcTimeout = Duration.ofSeconds(15);

    /**
     * 为配置日志保留令牌首尾，避免输出完整访问凭据。
     *
     * @return 掩码后的令牌，短令牌全部隐藏。
     */
    @ToString.Include(name = "token")
    private String maskedToken() {
        boolean emptyToken = token == null || token.isEmpty();
        if (emptyToken) {
            return token;
        }

        int visibleLength = TOKEN_VISIBLE_LENGTH * 2;
        if (token.length() <= visibleLength) {
            return TOKEN_MASK;
        }

        String prefix = token.substring(0, TOKEN_VISIBLE_LENGTH);
        String suffix = token.substring(token.length() - TOKEN_VISIBLE_LENGTH);
        return prefix + TOKEN_MASK + suffix;
    }
}
