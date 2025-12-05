package com.wolfhouse.influxclient.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

/**
 * 时间戳工具
 *
 * @author rylinwolf
 */
@Slf4j
public class TimeStampUtils {

    /**
     * 将各种精度的时间戳转换为 Instant
     *
     * @param timestamp 时间戳
     * @param unit      时间单位
     * @return Instant
     */
    public static Instant toInstant(long timestamp, ChronoUnit unit) {
        return switch (unit) {
            case NANOS -> Instant.ofEpochSecond(
                    timestamp / 1_000_000_000L,
                    timestamp % 1_000_000_000L
            );
            case MICROS -> Instant.ofEpochSecond(
                    timestamp / 1_000_000L,
                    (timestamp % 1_000_000L) * 1_000L
            );
            case MILLIS -> Instant.ofEpochMilli(timestamp);
            case SECONDS -> Instant.ofEpochSecond(timestamp);
            default -> throw new IllegalArgumentException("不支持的 ChronoUnit: " + unit);
        };
    }

    /**
     * 自动检测并转换
     */
    public static Instant autoConvert(long timestamp) {
        Instant instant = detectAndConvert(timestamp);
        log.debug("转换时间: {}, 结果: {}", timestamp, formatInstant(instant));
        log.debug("对应北京时间: {}", formatInBeijing(instant));
        return instant;
    }

    private static Instant detectAndConvert(long timestamp) {
        // 根据数值范围判断
        if (timestamp >= 1_000_000_000_000_000_000L) {
            // >= 1e18
            // 19位数字，应该是纳秒
            return toInstant(timestamp, ChronoUnit.NANOS);
        } else if (timestamp >= 1_000_000_000_000_000L) {
            // >= 1e15
            // 16位数字，可能是纳秒或微秒
            return tryBoth(timestamp);
        } else if (timestamp >= 1_000_000_000_000L) {
            // >= 1e12
            // 13位数字，通常是毫秒
            return toInstant(timestamp, ChronoUnit.MILLIS);
        } else if (timestamp >= 1_000_000_000L) {
            // >= 1e9
            // 10位数字，通常是秒
            return toInstant(timestamp, ChronoUnit.SECONDS);
        } else {
            throw new IllegalArgumentException("无法识别的时间戳: " + timestamp);
        }
    }

    private static Instant tryBoth(long timestamp) {
        // 尝试纳秒
        Instant       nanosInstant = toInstant(timestamp, ChronoUnit.NANOS);
        LocalDateTime nanosTime    = LocalDateTime.ofInstant(nanosInstant, ZoneOffset.UTC);

        // 尝试微秒
        Instant       microsInstant = toInstant(timestamp, ChronoUnit.MICROS);
        LocalDateTime microsTime    = LocalDateTime.ofInstant(microsInstant, ZoneOffset.UTC);

        // 选择合理的年份
        int nanosYear  = nanosTime.getYear();
        int microsYear = microsTime.getYear();

        if (nanosYear >= 1970 && nanosYear <= 2200) {
            return nanosInstant;
        } else if (microsYear >= 1970 && microsYear <= 2200) {
            return microsInstant;
        } else {
            throw new IllegalArgumentException("无法确定时间戳单位: " + timestamp);
        }
    }

    /**
     * 格式化显示
     */
    public static String formatInstant(Instant instant) {
        return DateTimeFormatter.ISO_INSTANT.format(instant);
    }

    /**
     * 转换为北京时间
     */
    public static String formatInBeijing(Instant instant) {
        ZoneId        beijingZone = ZoneId.of("Asia/Shanghai");
        ZonedDateTime beijingTime = instant.atZone(beijingZone);
        return DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                .format(beijingTime);
    }

    /**
     * 主方法测试
     */
    public static void main(String[] args) {
        long timestamp = 1764910799518328000L;

        System.out.println("原始时间戳: " + timestamp);
        System.out.println("时间戳长度: " + String.valueOf(timestamp).length() + " 位");

        // 尝试不同单位的转换
        System.out.println("\n=== 不同单位的转换结果 ===");

        // 纳秒转换
        Instant nanosInstant = toInstant(timestamp, ChronoUnit.NANOS);
        System.out.println("作为纳秒: " + formatInstant(nanosInstant));
        System.out.println("  北京时间: " + formatInBeijing(nanosInstant));

        // 微秒转换
        Instant microsInstant = toInstant(timestamp, ChronoUnit.MICROS);
        System.out.println("\n作为微秒: " + formatInstant(microsInstant));
        System.out.println("  北京时间: " + formatInBeijing(microsInstant));

        // 毫秒转换
        Instant millisInstant = toInstant(timestamp, ChronoUnit.MILLIS);
        System.out.println("\n作为毫秒: " + formatInstant(millisInstant));
        System.out.println("  北京时间: " + formatInBeijing(millisInstant));

        // 自动检测
        System.out.println("\n=== 自动检测结果 ===");
        autoConvert(timestamp);
    }
}