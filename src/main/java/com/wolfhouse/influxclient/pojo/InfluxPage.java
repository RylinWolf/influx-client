package com.wolfhouse.influxclient.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * 分页对象
 *
 * @author Rylin Wolf
 */
@Data
@Builder
@Accessors(chain = true, fluent = true)
@NoArgsConstructor
@AllArgsConstructor
public class InfluxPage<T> {
    private long    pageNum;
    private long    pageSize;
    private long    total;
    private List<T> records;
}
