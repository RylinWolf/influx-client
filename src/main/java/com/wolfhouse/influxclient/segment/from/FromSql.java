package com.wolfhouse.influxclient.segment.from;

import com.wolfhouse.influxclient.segment.InfluxBuiltInTableMeta;

/**
 * 特殊的用于 from 语句后的 SQL 字段
 *
 * @author Rylin Wolf
 */
public class FromSql {
    public static String lastCache(String measurement, String cacheTable) {
        return "last_cache('%s', '%s')".formatted(measurement, cacheTable);
    }

    public static String systemLastCache() {
        return InfluxBuiltInTableMeta.TABLE_LAST_CACHE;
    }
}
