package com.wolfhouse.influxclient.typehandler;

/**
 * @author Rylin Wolf
 */
public class IntTypeHandler implements TypeHandler<Integer> {
    @Override
    public Integer getResult(Object result) {
        if (result == null) {
            return null;
        }
        if (result instanceof Number) {
            return ((Number) result).intValue();
        }
        return (int) (result.toString().charAt(0));
    }
}
