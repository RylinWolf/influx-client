package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.core.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InfluxClient {
    public final InfluxDBClientImpl client;

    public <T extends AbstractActionInfluxObj> void insert(T obj) {
        client.writePoint(PointBuilder.build(obj));
    }

    public <T extends AbstractActionInfluxObj> void insertAll(Collection<T> objs) {
        client.writePoints(PointBuilder.buildAll(objs));
    }

    public Stream<Object[]> query(String sql, Map<String, Object> parameters) {
        if (parameters != null) {
            log.debug("执行查询: {}\n参数集: {}", sql, parameters);
            return client.query(sql, parameters);
        }
        log.debug("执行查询: {}", sql);
        return client.query(sql);
    }

    public Stream<Object[]> query(InfluxQueryWrapper<?> wrapper) {
        return query(wrapper.build(), wrapper.getConditionWrapper().getParameters());
    }

    public <T extends AbstractBaseInfluxObj> Collection<T> queryMap(InfluxQueryWrapper<?> wrapper, Class<T> clazz) {
        return InfluxObjMapper.mapAll(query(wrapper), clazz, wrapper);
    }
}
