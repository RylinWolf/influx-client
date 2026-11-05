package com.wolfhouse.influxclient.utils;

import com.wolfhouse.influxclient.typehandler.InstantTypeHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证支持的时间精度和 SDK 纳秒时间列转换。
 *
 * @author Rylin Wolf
 */
class TimeStampUtilsTest {
    /** 微秒精度的现代时间戳，用于验证已停止支持该精度 */
    private static final long UNSUPPORTED_MICROSECONDS = 1_735_689_600_000_000L;

    /**
     * 验证常见秒、毫秒和纳秒时间戳均能对应同一时间点。
     *
     * @param timestamp 时间戳数值。
     * @param unit 明确指定的时间单位。
     */
    @ParameterizedTest
    @CsvSource({"1735689600, SECONDS", "1735689600000, MILLIS", "1735689600000000000, NANOS"})
    void convertsSupportedUnits(long timestamp, ChronoUnit unit) {
        Instant expected = Instant.parse("2025-01-01T00:00:00Z");
        Instant negativeExpected = Instant.ofEpochSecond(-expected.getEpochSecond());

        assertEquals(expected, TimeStampUtils.toInstant(timestamp, unit));
        assertEquals(expected, TimeStampUtils.autoConvert(timestamp));
        assertEquals(negativeExpected, TimeStampUtils.autoConvert(-timestamp));
    }

    /** 验证显式转换和自动转换均不再接受常见微秒时间戳。 */
    @Test
    void microsecondsAreUnsupported() {
        assertThrows(IllegalArgumentException.class,
                () -> TimeStampUtils.toInstant(UNSUPPORTED_MICROSECONDS, ChronoUnit.MICROS));
        assertThrows(IllegalArgumentException.class,
                () -> TimeStampUtils.autoConvert(UNSUPPORTED_MICROSECONDS));
    }

    /** 验证 SDK 的小数值纳秒时间戳按已知精度处理，不进入自动猜测。 */
    @Test
    void sdkNanosecondsHandleEpochAndSubsecondValues() {
        InstantTypeHandler handler = new InstantTypeHandler();

        assertEquals(Instant.EPOCH, handler.getResult(0L));
        assertEquals(Instant.EPOCH.plusNanos(1), handler.getResult(1L));
        assertEquals(Instant.EPOCH.minusNanos(1), handler.getResult(-1L));
        assertNull(handler.getResult(null));
        assertEquals(Instant.EPOCH, TimeStampUtils.autoConvert(0L));
    }
}
