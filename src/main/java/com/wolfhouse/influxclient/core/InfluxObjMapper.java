package com.wolfhouse.influxclient.core;

import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxResult;
import com.wolfhouse.influxclient.typehandler.InfluxTypeHandler;
import com.wolfhouse.influxclient.typehandler.TypeHandler;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Influx 对象转换器
 *
 * @author Rylin Wolf
 */
@Slf4j
public class InfluxObjMapper {
    /**
     * 缓存 TypeHandler 实例，避免重复反射创建
     */
    private static final Map<Class<? extends TypeHandler<?>>, TypeHandler<?>> HANDLER_CACHE = new ConcurrentHashMap<>();

    /**
     * 将一个对象数组流映射为指定类型的对象实例列表。
     *
     * @param <T>       指定的目标类型，必须继承自 AbstractBaseInfluxObj
     * @param <Wrapper> 包装类类型，必须继承自 InfluxQueryWrapper
     * @param objStream 对象数组流，每个数组表示一个记录的数据
     * @param clazz     目标类的 Class 对象，用于反射实例化目标类型对象
     * @param wrapper   查询包装器，用于获取映射的字段名称集合
     * @return 映射后的目标类型对象列表
     * @throws RuntimeException 如果映射失败、无法实例化目标对象或其他错误发生时抛出
     */
    public static <T extends AbstractBaseInfluxObj, Wrapper extends InfluxQueryWrapper<?>> List<T> mapAll(Stream<Object[]> objStream, Class<T> clazz, Wrapper wrapper) {
        return objStream.map(obj -> map(obj, clazz, wrapper.getMixedTargetsWithAlias())).toList();
    }

    /**
     * 将一个对象数组映射为指定类型的对象实例。
     *
     * @param <T>     指定的目标类型，必须继承自 {@link AbstractBaseInfluxObj}
     * @param obj     用于映射的对象数组
     * @param clazz   目标类的 Class 对象
     * @param targets 字段名称集合，用于指定映射时的字段顺序，需与对象数组的元素数量一致
     * @return 映射后的目标类型对象
     * @throws RuntimeException 如果字段注入失败、无法实例化目标对象或其他错误发生时抛出
     */
    @SuppressWarnings("unchecked")
    public static <T extends AbstractBaseInfluxObj> T map(Object[] obj,
                                                          Class<T> clazz,
                                                          SequencedCollection<String> targets) {
        // 对于 InfluxResult 特殊处理
        if (clazz.equals(InfluxResult.class)) {
            return (T) mapToResult(obj, targets);
        }
        try {
            // 创建目标类对象
            T t = clazz.getDeclaredConstructor().newInstance();
            // 获取查询参数，与对象数组一一对应
            targets = new LinkedHashSet<>(targets);
            assert targets.size() == obj.length : "查询参数数与结果集不一致！";

            for (Object o : obj) {
                // 当前值的字段名
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
                Object valueToSet = handleType(o, field);
                field.set(t, valueToSet);
            }
            return t;

        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 对字段进行类型处理，如果字段标注了 {@link InfluxTypeHandler}，则使用对应的 {@link TypeHandler} 对输入值进行转换。
     * 处理完成后返回转换后的值。
     *
     * @param o     输入的对象值，需要被处理的值
     * @param field 对应字段对象，用于检查是否存在注解 {@link InfluxTypeHandler} 以及获取注解信息
     * @return 转换后的对象值，如果字段未标注注解 {@link InfluxTypeHandler}，则返回输入值 o 本身
     * @throws RuntimeException 如果处理过程中实例化 {@link TypeHandler} 或转换失败，则抛出异常
     */
    private static Object handleType(Object o, Field field) {
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
        return valueToSet;
    }

    public static <Wrapper extends InfluxQueryWrapper<?>> List<Map<String, Object>> compressToMapList(Stream<Object[]> objs, Wrapper wrapper) {
        return objs.map(obj -> compressToMap(obj, wrapper.getMixedTargetsWithAlias())).toList();
    }

    /**
     * 将一个对象数组流压缩并映射为一个包含映射关系的列表，每个列表项为一个键值对的映射表。
     * 方法将通过指定的字段名称集合，将对象数组的每个元素与字段名逐一匹配进行映射。
     *
     * @param objs    对象数组流，其中每个对象数组表示一组数据记录
     * @param targets 用于映射的字段名称集合，字段顺序需与对象数组中的数据顺序一致
     * @return 映射后的键值对列表，如果出现字段数量与记录列数不一致，将返回null并记录错误日志
     */
    public static List<Map<String, Object>> compressToMapList(Stream<Object[]> objs, final SequencedCollection<String> targets) {
        String[] targetsArray = targets.toArray(String[]::new);
        return objs.map(obj -> {
            // 要查询的参数数量与返回的结果集字段数量不一致
            if (obj.length != targetsArray.length) {
                log.error("查询参数数与结果集不一致: {}, {}", obj, targetsArray);
                return null;
            }
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < obj.length; i++) {
                // 按位置匹配当前字段与字段名
                map.put(targetsArray[i], obj[i]);
            }
            return map;
        }).toList();
    }

    public static Map<String, Object> compressToMap(Object[] obj, final SequencedCollection<String> targets) {
        return compressToMapList(Stream.<Object[]>of(obj), targets).getFirst();
    }

    public static InfluxResult mapToResult(Object[] obj, SequencedCollection<String> targets) {
        return new InfluxResult().addRow(compressToMap(obj, targets));
    }

    /**
     * 将对象数组流映射为 InfluxResult 类型的实例。
     * 通过指定字段名称集合，将对象数组流的数据逐一映射为键值对列表，
     * 然后构造并返回封装后的查询结果对象。
     *
     * @param objStream 对象数组流，其中每个数组表示一条数据记录
     * @param targets   字段名称集合，用于指定映射时的字段顺序，与对象数组中数据的顺序需一致
     * @return 封装后的 InfluxResult 对象，包含所有映射后的数据记录
     */
    public static InfluxResult mapAllToResult(Stream<Object[]> objStream, SequencedCollection<String> targets) {
        return new InfluxResult().addAllRow(compressToMapList(objStream, targets));
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
