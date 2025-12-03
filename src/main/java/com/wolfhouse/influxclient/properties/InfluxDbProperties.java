package com.wolfhouse.influxclient.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Rylin Wolf
 */
@ConfigurationProperties(prefix = "influx")
@Configuration
@Data
public class InfluxDbProperties {
    private String token = System.getenv("INFLUX_TOKEN");
    private String url;
    private String database;
}
