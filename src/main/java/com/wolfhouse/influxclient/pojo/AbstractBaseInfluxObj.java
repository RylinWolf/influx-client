package com.wolfhouse.influxclient.pojo;

import com.wolfhouse.influxclient.typehandler.InfluxTypeHandler;
import com.wolfhouse.influxclient.typehandler.InstantTypeHandler;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;

/**
 * @author Rylin Wolf
 */
@Getter
@ToString
@SuppressWarnings({"UnusedReturnValue", "unused"})
public abstract class AbstractBaseInfluxObj {
    /** InfluxDB的度量名称，用于指定数据写入的表 */
    protected final String  measurement;
    /** 数据点的时间戳 */
    @Setter
    @InfluxTypeHandler(InstantTypeHandler.class)
    protected       Instant time;

    protected AbstractBaseInfluxObj() {
        this.time        = Instant.now();
        this.measurement = tableName();
    }

    /**
     * 设置 InfluxDB 对象对应的表名
     *
     * @return 表名
     */
    protected abstract String tableName();

    /**
     * 使用当前的时间，刷新时间戳数据
     */
    public void refreshTimestamp() {
        this.time = Instant.now();
    }
}
