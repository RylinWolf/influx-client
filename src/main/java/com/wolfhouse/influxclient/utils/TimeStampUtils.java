package com.wolfhouse.influxclient.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * 时间戳工具，支持秒、毫秒和纳秒精度。
 *
 * @author Rylin Wolf
 */
public class TimeStampUtils {
    /** 每秒包含的纳秒数 */
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    /** 常见毫秒时间戳的绝对值下界 */
    private static final long AUTO_MILLIS_MIN = 100_000_000_000L;
    /** 自动识别允许的毫秒时间戳绝对值上界，不包含该值 */
    private static final long AUTO_MILLIS_LIMIT = 100_000_000_000_000L;
    /** 常见纳秒时间戳的绝对值下界 */
    private static final long AUTO_NANOS_MIN = 100_000_000_000_000_000L;
    /** 独立运行示例使用的纳秒时间戳 */
    private static final long EXAMPLE_NANOS = 1_764_910_799_518_328_000L;
    /** 北京时间使用的时区 */
    private static final ZoneId BEIJING_ZONE = ZoneId.of("Asia/Shanghai");
    /** 北京时间的毫秒级显示格式 */
    private static final DateTimeFormatter BEIJING_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * 按明确指定的秒、毫秒或纳秒精度转换。
     *
     * @param timestamp 时间戳。
     * @param unit 时间单位。
     * @return 对应的时间点。
     */
    public static Instant toInstant(long timestamp, ChronoUnit unit) {
        return switch (unit) {
            case NANOS -> Instant.ofEpochSecond(timestamp / NANOS_PER_SECOND, timestamp % NANOS_PER_SECOND);
            case MILLIS -> Instant.ofEpochMilli(timestamp);
            case SECONDS -> Instant.ofEpochSecond(timestamp);
            default -> throw new IllegalArgumentException("【TimeStampUtils】仅支持秒、毫秒和纳秒，当前单位为 " + unit);
        };
    }

    /**
     * 按常见时间戳的数量级识别秒、毫秒或纳秒，不支持微秒。
     * 低数值的历史时间戳应通过 {@link #toInstant(long, ChronoUnit)} 指定精度。
     *
     * @param timestamp 待识别的时间戳。
     * @return 对应的时间点。
     */
    public static Instant autoConvert(long timestamp) {
        boolean nanoseconds = timestamp >= AUTO_NANOS_MIN || timestamp <= -AUTO_NANOS_MIN;
        if (nanoseconds) {
            return toInstant(timestamp, ChronoUnit.NANOS);
        }

        boolean unsupportedRange = timestamp >= AUTO_MILLIS_LIMIT || timestamp <= -AUTO_MILLIS_LIMIT;
        if (unsupportedRange) {
            throw new IllegalArgumentException("【TimeStampUtils】不支持微秒，其他时间戳请明确指定秒、毫秒或纳秒精度");
        }

        boolean milliseconds = timestamp >= AUTO_MILLIS_MIN || timestamp <= -AUTO_MILLIS_MIN;
        if (milliseconds) {
            return toInstant(timestamp, ChronoUnit.MILLIS);
        }
        return toInstant(timestamp, ChronoUnit.SECONDS);
    }

    /**
     * 格式化为 ISO 时间文本。
     *
     * @param instant 待格式化的时间点。
     * @return ISO 时间文本。
     */
    public static String formatInstant(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    /**
     * 格式化为北京时间。
     *
     * @param instant 待格式化的时间点。
     * @return 精确到毫秒的北京时间文本。
     */
    public static String formatInBeijing(Instant instant) {
        return BEIJING_FORMAT.format(instant.atZone(BEIJING_ZONE));
    }

    /**
     * 独立运行时展示纳秒时间戳的转换结果。
     *
     * @param args 命令行参数。
     */
    public static void main(String[] args) {
        Instant instant = toInstant(EXAMPLE_NANOS, ChronoUnit.NANOS);
        System.out.println("纳秒时间戳 " + EXAMPLE_NANOS);
        System.out.println("ISO 时间 " + formatInstant(instant));
        System.out.println("北京时间 " + formatInBeijing(instant));
    }
}
