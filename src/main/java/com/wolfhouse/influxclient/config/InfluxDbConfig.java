package com.wolfhouse.influxclient.config;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * InfluxDB 配置类，仅在配置了 influx.url 后生效
 *
 * @author Rylin Wolf
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "influx", name = "url")
public class InfluxDbConfig {
    /**
     * 初始化 InfluxDB 客户端的方法。
     *
     * @param properties 包含 InfluxDB 客户端必要配置信息的配置类对象，包括 URL、Token 和数据库名。
     * @return 初始化成功返回 InfluxDB 客户端对象 {@link InfluxDBClientImpl}，如果 Token 为空或初始化失败则返回 null。
     */
    @Bean(destroyMethod = "close")
    public InfluxDBClientImpl influxDbClient(InfluxDbProperties properties) {
        log.info("【Influx DB】初始化 InfluxDB 客户端... {}", properties.getUrl());
        String token = properties.getToken();
        if (token == null || token.isEmpty()) {
            log.error("【Influx DB】客户端未初始化，Token 为空");
            return null;
        }

        // 创建客户端并尝试建立连接
        try {
            InfluxDBClientImpl client        = (InfluxDBClientImpl) InfluxDBClient.getInstance(properties.getUrl(), token.toCharArray(), properties.getDatabase());
            String             serverVersion = client.getServerVersion();
            log.info("【Influx DB】客户端初始化完成，版本: {}", serverVersion);
            return client;
        } catch (Exception exception) {
            log.error("【Influx DB】客户端初始化失败: {}", exception.getMessage(), exception);
            return null;
        }
    }
}
