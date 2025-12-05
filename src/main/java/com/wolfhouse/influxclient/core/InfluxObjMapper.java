package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.typehandler.InfluxTypeHandler;
import com.wolfhouse.influxclient.typehandler.TypeHandler;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@Slf4j
public class InfluxObjMapper {
    /**
     * 缓存 TypeHandler 实例，避免重复反射创建
     */
    private static final Map<Class<? extends TypeHandler<?>>, TypeHandler<?>> HANDLER_CACHE = new ConcurrentHashMap<>();

    public static <T extends AbstractInfluxObj, Wrapper extends InfluxQueryWrapper<T>> List<T> mapAll(Stream<Object[]> objStream, Class<T> clazz, Wrapper wrapper) {
        return objStream.map(obj -> map(obj, clazz, wrapper.getQueryTargets())).toList();
    }

    public static <T extends AbstractInfluxObj> T map(Object[] obj,
                                                      Class<T> clazz,
                                                      SequencedCollection<String> targets) {
        try {
            // 创建目标类对象
            T t = clazz.getDeclaredConstructor().newInstance();
            // 获取查询参数，与对象数组一一对应
            targets = new LinkedHashSet<>(targets);
            assert targets.size() == obj.length : "查询参数数与结果集不一致！";

            for (Object o : obj) {
                String name = targets.removeFirst();
                // 将蛇形命名转换为驼峰命名，以便匹配 Java 字段
                String fieldName = toCamelCase(name);
                // 尝试获取字段并注入
                Field field = getField(clazz, fieldName);
                if (field == null) {
                    // 如果驼峰命名找不到，尝试使用原名（兼容本来就是驼峰或不需要转换的情况）
                    field = getField(clazz, name);
                }

                if (field == null) {
                    // 无该字段，直接跳过
                    log.warn("【InfluxObjectMapper】尝试注入字段失败，类 {} 不包含该字段: {} 或 {}", clazz, fieldName, name);
                    continue;
                }
                // 这里字段一定存在
                field.setAccessible(true);
                // 处理自定义 TypeHandler
                Object valueToSet = o;
                if (field.isAnnotationPresent(InfluxTypeHandler.class)) {
                    InfluxTypeHandler               annotation   = field.getAnnotation(InfluxTypeHandler.class);
                    Class<? extends TypeHandler<?>> handlerClass = annotation.value();
                    try {
                        // 从缓存获取或新建 Handler 实例
                        TypeHandler<?> handler = HANDLER_CACHE.computeIfAbsent(handlerClass,
                                k -> {
                                    try {
                                        return k.getDeclaredConstructor().newInstance();
                                    } catch (Exception e) {
                                        throw new RuntimeException("无法实例化 TypeHandler: " + k.getName(), e);
                                    }
                                });
                        // 执行转换
                        valueToSet = handler.getResult(o);
                    } catch (Exception e) {
                        log.error("TypeHandler 转换失败，字段: {}, Handler: {}", field.getName(), handlerClass.getName(), e);
                        throw new RuntimeException(e);
                    }
                }
                field.set(t, valueToSet);
            }
            return t;

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 递归查找字段（支持 private 和父类字段）
     */
    private static Field getField(Class<?> clazz, String fieldName) {
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        return null;
    }


    /**
     * 将下划线分割的命名（snake_case）转换为驼峰命名（lowerCamelCase）
     * 例如：sensor_id -> sensorId
     */
    private static String toCamelCase(String s) {
        if (s == null) {
            return null;
        }
        StringBuilder sb        = new StringBuilder();
        boolean       upperCase = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '_') {
                upperCase = true;
            } else {
                if (upperCase) {
                    sb.append(Character.toUpperCase(c));
                    upperCase = false;
                } else {
                    sb.append(c);
                }
            }
        }
        return sb.toString();
    }
}
