package com.wolfhouse.influxclient.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

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
}
