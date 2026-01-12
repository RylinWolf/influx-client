package com.wolfhouse.influxclient.constant;

/**
 * InfluxDB 内置表信息
 *
 * @author Rylin Wolf
 */
public interface InfluxBuiltInTableMeta {
    /** 表 字段名 */
    String MEASUREMENT_TAG              = "measurement";
    /** 时间 字段名 */
    String TIME_TAG                     = "time";
    /** 列名字段名 */
    String COLUMN_META_COLUMN_NAME      = "column_name";
    /** 数据类型字段名 */
    String COLUMN_TYPE                  = "data_type";
    /** 列信息表名 */
    String COLUMN_META_MEASUREMENT      = "information_schema.columns";
    /** 列信息 表名字段名 */
    String COLUMN_META_TABLE_NAME_FIELD = "table_name";
}
