package com.wolfhouse.influxclient.pojo.system;

import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.typehandler.InfluxTypeHandler;
import com.wolfhouse.influxclient.typehandler.IntTypeHandler;
import com.wolfhouse.influxclient.typehandler.SetTypeHandler;
import com.wolfhouse.influxclient.typehandler.TextTypeHandler;
import lombok.*;

import java.util.Set;

/**
 * 系统内置表 - LVC 表
 *
 * @author Rylin Wolf
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Builder
public class LastCaches extends AbstractActionInfluxObj {
    @InfluxTypeHandler(TextTypeHandler.class)
    private String       table;
    @InfluxTypeHandler(TextTypeHandler.class)
    private String       name;
    @InfluxTypeHandler(value = SetTypeHandler.class, subTypeHandler = IntTypeHandler.class)
    private Set<Integer> keyColumnIds;
    @InfluxTypeHandler(value = SetTypeHandler.class)
    private Set<String>  keyColumnNames;
    @InfluxTypeHandler(value = SetTypeHandler.class, subTypeHandler = IntTypeHandler.class)
    private Set<Integer> valueColumnIds;
    @InfluxTypeHandler(SetTypeHandler.class)
    private Set<String>  valueColumnNames;
    /** 缓存记录数量（最近几条记录） */
    private Long         count;
    /** 缓存过期时间，单位：秒 */
    private Long         ttl;
}
