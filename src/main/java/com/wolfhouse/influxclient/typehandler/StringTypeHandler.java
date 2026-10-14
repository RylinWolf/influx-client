package com.wolfhouse.influxclient.typehandler;

/**
 * @author Rylin Wolf
 */
public class StringTypeHandler implements TypeHandler<String> {
    @Override
    public String getResult(Object result) {
        return result == null ? "" : result.toString();
    }
}
