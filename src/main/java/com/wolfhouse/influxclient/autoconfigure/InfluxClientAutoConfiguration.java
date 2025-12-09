package com.wolfhouse.influxclient.autoconfigure;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.client.InfluxClient;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * InfluxClient及其底层InfluxDB客户端的自动配置。
 * <p>
 * 当提供`influx.url`属性时，此配置将被激活。
 *
 * @author Rylin Wolf
 */
@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(InfluxDbProperties.class)
@ConditionalOnClass({InfluxClient.class, InfluxDBClient.class})
@ConditionalOnProperty(prefix = "influx", name = "url")
public class InfluxClientAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(InfluxDBClientImpl.class)
    public InfluxDBClientImpl influxDbClient(InfluxDbProperties properties) {
        log.info("[InfluxClientStarter] 正在初始化InfluxDB客户端... {}", properties.getUrl());
        String token = properties.getToken();
        if (token == null || token.isEmpty()) {
            log.error("[InfluxClientStarter] Token为空，InfluxDB客户端将不会被初始化");
            return null;
        }
        try {
            InfluxDBClientImpl client = (InfluxDBClientImpl) InfluxDBClient.getInstance(
                    properties.getUrl(), token.toCharArray(), properties.getDatabase());
            log.info("[InfluxClientStarter] InfluxDB服务器版本: {}", client.getServerVersion());
            return client;
        } catch (Exception e) {
            log.error("[InfluxClientStarter] InfluxDB客户端初始化失败: {}", e.getMessage(), e);
            return null;
        }
    }

    @Bean
    @ConditionalOnMissingBean(InfluxClient.class)
    public InfluxClient influxClient(InfluxDBClientImpl influxDbClient) {
        return new InfluxClient(influxDbClient);
    }
}
