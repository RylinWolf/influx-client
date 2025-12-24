package com.wolfhouse.influxclient.pojo;

import lombok.Getter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB 结果封装类，用于封装查询结果集
 * <p>
 * 注意，该查询结果类基于 map，且
 *
 * @author Rylin Wolf
 */
@SuppressWarnings({"UnusedReturnValue", "unused"})
@ToString
public class InfluxResult extends AbstractBaseInfluxObj {
    protected List<InfluxRow> results = new ArrayList<>();

    {
        // 应该依赖于数据的时间结果，因此这里的时间对象是无效的
        super.time(null);
    }

    public InfluxResult addRow(Map<String, Object> result) {
        this.results.add(new InfluxRow().setRowRecord(result));
        return this;
    }

    public InfluxResult addAllRow(List<Map<String, Object>> results) {
        results.forEach(this::addRow);
        return this;
    }

    public Map<String, Object> row(int index) {
        return this.results.get(index).getRowRecord();
    }

    public List<InfluxRow> rows() {
        return this.results;
    }


    public static class InfluxRow {
        @Getter
        private Map<String, Object> rowRecord;

        public InfluxRow setRowRecord(Map<String, Object> rowRecord) {
            this.rowRecord = rowRecord;
            return this;
        }

        public InfluxRow addRecord(Map<String, Object> result) {
            this.rowRecord.putAll(result);
            return this;
        }

        public InfluxRow addRecord(String key, Object value) {
            this.rowRecord.put(key, value);
            return this;
        }

        @Override
        public String toString() {
            return rowRecord.toString();
        }
    }
}
