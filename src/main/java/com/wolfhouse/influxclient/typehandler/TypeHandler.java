package com.wolfhouse.influxclient.typehandler;

/**
 * 类型转换处理器接口
 *
 * @param <T> 目标 Java 类型
 * @author Rylin Wolf
 */
public interface TypeHandler<T> {
    /**
     * 将 InfluxDB 返回的原始值转换为目标类型
     *
     * @param result InfluxDB 返回的原始值 (通常是 String, Double, Instant 等)
     * @return 转换后的 Java 对象
     */
    T getResult(Object result);
}