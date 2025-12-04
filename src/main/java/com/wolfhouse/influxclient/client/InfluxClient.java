package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.core.AbstractInfluxObj;
import com.wolfhouse.influxclient.core.PointBuilder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InfluxClient {
    public final InfluxDBClientImpl client;

    public <T extends AbstractInfluxObj> void insert(T obj) {
        log.info("写入数据...");
        client.writePoint(PointBuilder.build(obj));
    }

    public <T extends AbstractInfluxObj> void insertAll(Collection<T> objs) {
        log.info("批量写入数据...");
        client.writePoints(PointBuilder.buildAll(objs));
    }

    public void test() {
        String           sql   = "select location, value, time from temperature order by time desc limit 10";
        Stream<Object[]> query = client.query(sql);
        query.forEach(o -> System.out.printf("| %-8s | %-8s | %-30s |%n", o[0], o[1], o[2]));
    }

}
