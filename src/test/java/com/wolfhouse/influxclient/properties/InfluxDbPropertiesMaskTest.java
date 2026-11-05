package com.wolfhouse.influxclient.properties;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 验证配置日志对短令牌和空令牌的处理。
 *
 * @author Rylin Wolf
 */
class InfluxDbPropertiesMaskTest {
    /**
     * 验证短令牌全部掩码，包含保留首尾字符的长度边界。
     *
     * @param token 测试使用的短令牌。
     */
    @ParameterizedTest
    @ValueSource(strings = {"a", "abcd", "abcdefgh"})
    void shortTokenIsFullyMasked(String token) {
        InfluxDbProperties properties = new InfluxDbProperties();
        properties.setToken(token);

        assertTrue(properties.toString().contains("token=****"));
        assertFalse(properties.toString().contains("token=" + token + ","));
        assertEquals(token, properties.getToken());
    }

    /**
     * 验证空令牌的日志输出不会抛异常。
     *
     * @param token 测试使用的空令牌。
     */
    @ParameterizedTest
    @NullAndEmptySource
    void absentTokenCanBeLogged(String token) {
        InfluxDbProperties properties = new InfluxDbProperties();
        properties.setToken(token);

        assertDoesNotThrow(properties::toString);
        assertEquals(token, properties.getToken());
    }
}
