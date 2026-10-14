package com.wolfhouse.influxclient.typehandler;

import com.wolfhouse.influxclient.utils.ServiceUtils;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * 集类型处理器
 *
 * @author Rylin Wolf
 */
public class SetTypeHandler implements CollectionTypeHandler<Set<?>> {
    @Override
    public <T> Set<T> getCollection(Object result, TypeHandler<T> subTypeHandler) {
        @SuppressWarnings("rawtypes")
        Set resultSet = ServiceUtils.getObjectMapper().convertValue(result, Set.class);
        //noinspection unchecked
        return (Set<T>) resultSet.stream().map(subTypeHandler::getResult).collect(Collectors.toSet());
    }
}
