package com.wolfhouse.influxclient.anno;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * 该注解用于：转换结果集为指定对象时，标识某个集合，
 * 将反射时未能匹配到的字段存储入标识的集合中
 *
 * @author Rylin Wolf
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface OtherColumns {
}
