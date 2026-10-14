package com.wolfhouse.influxclient.typehandler;

import java.util.Collection;

/**
 * @author Rylin Wolf
 */
public interface CollectionTypeHandler<C extends Collection<?>> extends TypeHandler<C> {

    /**
     * 弃用
     *
     * @param result InfluxDB 返回的原始值 (通常是 String, Double, Instant 等)
     * @return 未实现，请使用 {@link #getCollection(Object, TypeHandler)}
     */
    @Override
    default C getResult(Object result) {
        throw new UnsupportedOperationException("use getCollection instead");
    }

    /**
     * 转换集合
     *
     * @param result         InfluxDB 返回的原始值 (通常是 String, Double, Instant 等)
     * @param subTypeHandler 集合子类型处理器
     * @return 转换后的集合
     */
    <T> C getCollection(Object result, TypeHandler<T> subTypeHandler);
}
