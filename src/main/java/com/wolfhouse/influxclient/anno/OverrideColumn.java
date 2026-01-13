package com.wolfhouse.influxclient.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 该注解用于：从 InfluxDB 读取并转换对象时，默认不会覆盖已有初始值的对象。
 * 使用该注解可以标识某个字段或某个类的所有字段在有初始值时可以被覆盖。
 *
 * @author Rylin Wolf
 */
@Target({ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface OverrideColumn {

}
