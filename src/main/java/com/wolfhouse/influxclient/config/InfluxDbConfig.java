package com.wolfhouse.influxclient.config;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * InfluxDB 配置类
 *
 * @author Rylin Wolf
 */
@Slf4j
@Configuration
@ConfigurationPropertiesScan(basePackages = "com.wolfhouse.influxclient.properties")
public class InfluxDbConfig {
    @Bean(destroyMethod = "close")
    public InfluxDBClientImpl influxDbConfig(InfluxDbProperties properties) {
        String token = properties.getToken();
        if (token == null || token.isEmpty()) {
            log.error("【Influx DB】客户端未初始化，Token 为空");
            return null;
        }
        return (InfluxDBClientImpl) InfluxDBClient.getInstance(properties.getUrl(), token.toCharArray(), properties.getDatabase());
    }
}
