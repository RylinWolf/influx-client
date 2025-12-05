package com.wolfhouse.influxclient.typehandler;

import java.lang.annotation.*;

/**
 * 用于标记字段使用特定的 TypeHandler 进行类型转换
 *
 * @author Rylin Wolf
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface InfluxTypeHandler {
    /**
     * 指定使用的 TypeHandler 类
     */
    Class<? extends TypeHandler<?>> value();
}