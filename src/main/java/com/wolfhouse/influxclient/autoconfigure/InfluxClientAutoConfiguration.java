package com.wolfhouse.influxclient.autoconfigure;

import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.client.InfluxClient;
import com.wolfhouse.influxclient.client.InfluxClientProxy;
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
@ConditionalOnClass({InfluxClient.class, InfluxDBClient.class, InfluxClientProxy.class})
@ConditionalOnProperty(prefix = "influx", name = "url")
public class InfluxClientAutoConfiguration {
    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(InfluxClientProxy.class)
    public InfluxClientProxy influxDbClient(InfluxDbProperties properties) {
        return InfluxClientProxy.instance(properties);
    }
}
