package com.wolfhouse.influxclient.core;

/**
 * 基于 {@link AbstractInfluxObj} 的查询构建器
 *
 * @author Rylin Wolf
 */
public class InfluxQueryWrapper<T extends AbstractInfluxObj> {
    private String measurement;
    private T      reference;

    // region 构造方法

    private InfluxQueryWrapper(String measurement, T reference) {
        this.measurement = measurement;
        this.reference   = reference;
    }

    private InfluxQueryWrapper() {}

    // endregion

    // region 获取实例

    /**
     * 从指定的 {@link AbstractInfluxObj} 对象创建并初始化一个 {@link InfluxQueryWrapper} 实例。
     *
     * @param obj 提供初始化数据的对象，该对象必须继承自 {@link AbstractInfluxObj}。
     *            通过调用该对象的 {@code getMeasurement()} 方法获得测量名称。
     * @return 初始化好的 {@link InfluxQueryWrapper} 实例，其中包含传入对象的测量名称。
     */
    public static <T extends AbstractInfluxObj> InfluxQueryWrapper<T> from(T obj) {
        // TODO 未完成˚
        InfluxQueryWrapper<T> wrapper = new InfluxQueryWrapper<>();
        wrapper.measurement = obj.getMeasurement();
        return wrapper;
    }

    public static <T extends AbstractInfluxObj> InfluxQueryWrapper<T> fromAndBuild(T obj) {
        // TODO 未完成
        InfluxQueryWrapper<T> wrapper = new InfluxQueryWrapper<>();
        wrapper.measurement = obj.getMeasurement();
        return wrapper;
    }

    /**
     * 创建一个新的 {@link InfluxQueryWrapper} 实例，未初始化任何测量名称或引用对象。
     *
     * @return 一个新的 {@link InfluxQueryWrapper} 实例，测量名称和引用对象均为空。
     */
    public static InfluxQueryWrapper<?> create() {
        return new InfluxQueryWrapper<>(null, null);
    }
    // endregion


}
