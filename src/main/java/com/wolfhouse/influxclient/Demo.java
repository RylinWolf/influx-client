package com.wolfhouse.influxclient;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
public class Demo {
    /** your cluster URL */
    private static final String HOST_URL = "http://localhost:8181";
    /** your InfluxDB database name */
    private static final String DATABASE = "influx_client_demo";
    /** a local environment variable that stores your database token */
    private static final char[] TOKEN    = Optional.ofNullable(System.getenv("INFLUX_TOKEN"))
                                                   .orElse("")
                                                   .toCharArray();

    /** Create a client instance that writes and queries data in your database. */
    public static void main(String[] args) {
        // Instantiate the client with your InfluxDB credentials
        if (TOKEN.length == 0) {
            System.err.println("请设置环境变量 INFLUX_TOKEN");
            return;
        }
        try (InfluxDBClient client = InfluxDBClient.getInstance(HOST_URL, TOKEN, DATABASE)) {
            //writeData(client);
            queryData(client);
        } catch (Exception e) {
            System.err.println("An error occurred while connecting to InfluxDB!");
            e.printStackTrace();
        }
    }

    /** Use the Point class to construct time series data. */
    private static void writeData(InfluxDBClient client) {
        Point point = Point.measurement("temperature")
                           .setTag("location", "London")
                           .setField("value", 30.01)
                           .setTimestamp(Instant.now().minusSeconds(10));
        try {
            client.writePoint(point);
            System.out.println("Data is written to the database.");
        } catch (Exception e) {
            System.err.println("Failed to write data to the database.");
            e.printStackTrace();
        }
    }

    /** Use SQL to query the most recent 10 measurements */
    private static void queryData(InfluxDBClient client) {
        System.out.printf("--------------------------------------------------------%n");
        System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
        System.out.printf("--------------------------------------------------------%n");

        String sql = "select time,location,value from temperature order by time desc limit 10";
        try (Stream<Object[]> stream = client.query(sql)) {
            stream.forEach(row -> System.out.printf("| %-8s | %-8s | %-30s |%n", row[1], row[2], row[0]));
        } catch (Exception e) {
            System.err.println("Failed to query data from the database.");
            e.printStackTrace();
        }
    }
}
