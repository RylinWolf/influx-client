package com.wolfhouse.influxclient.typehandler;

/**
 * Text 类型转换器，处理 InfluxDB 映射为 Text 类型的字段
 *
 * @author Rylin Wolf
 */
public class TextTypeHandler implements TypeHandler<String> {
    @Override
    public String getResult(Object text) {
        return text.toString();
    }
}
