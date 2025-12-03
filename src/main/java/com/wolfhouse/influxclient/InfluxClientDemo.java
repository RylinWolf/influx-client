package com.wolfhouse.influxclient;

import com.wolfhouse.influxclient.client.InfluxClient;
import com.wolfhouse.influxclient.core.InfluxFields;
import com.wolfhouse.influxclient.core.InfluxTags;
import com.wolfhouse.influxclient.test.TestInsertObj;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author Rylin Wolf
 */
@SpringBootApplication
public class InfluxClientDemo {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(InfluxClientDemo.class, args);

        InfluxClient client = context.getBean(InfluxClient.class);
        System.out.println(client.client.getServerVersion());

        TestInsertObj obj = new TestInsertObj();
        obj.addTags(InfluxTags.from("location", "New York"));
        obj.addFields(InfluxFields.from("value", 100.01));
        client.insert(obj);

    }
}
