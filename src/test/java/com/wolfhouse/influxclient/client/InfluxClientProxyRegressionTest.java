package com.wolfhouse.influxclient.client;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 验证配置刷新失败恢复、连接替换顺序和令牌日志掩码。
 *
 * @author Rylin Wolf
 */
class InfluxClientProxyRegressionTest {
    /** 测试专用令牌，避免依赖或输出真实凭据 */
    private static final String TEST_TOKEN = "abcd-hidden-part-wxyz";
    /** 令牌在日志中的预期掩码结果 */
    private static final String MASKED_TOKEN = "abcd****wxyz";

    /** 验证底层新连接创建失败时，旧连接仍然保留并可被代理调用。 */
    @Test
    void connectionCreationFailurePreservesOldClient() {
        InfluxDBClientImpl oldDriver = mock(InfluxDBClientImpl.class);
        InfluxClient oldClient = mock(InfluxClient.class);
        InfluxClientProxy proxy = new InfluxClientProxy(oldDriver, oldClient);
        InfluxDbProperties properties = new InfluxDbProperties();
        properties.setToken(null);
        when(oldDriver.getServerVersion()).thenReturn("old");

        assertThrows(NullPointerException.class, () -> proxy.refreshClient(properties));
        assertSame(oldClient, proxy.client());
        assertEquals("old", proxy.getServerVersion());
        verify(oldClient, never()).close();
    }

    /**
     * 验证封装新客户端失败时，释放新连接且不关闭旧连接。
     *
     * @throws Exception 替身底层连接的关闭接口声明了受检异常。
     */
    @Test
    void wrapperCreationFailureClosesOnlyNewConnection() throws Exception {
        InfluxDBClientImpl oldDriver = mock(InfluxDBClientImpl.class);
        InfluxDBClientImpl newDriver = mock(InfluxDBClientImpl.class);
        InfluxClient oldClient = mock(InfluxClient.class);
        InfluxClientProxy proxy = new InfluxClientProxy(oldDriver, oldClient);
        InfluxDbProperties properties = new InfluxDbProperties();
        properties.setCacheFlushInterval(null);

        try (MockedStatic<InfluxClientProxy> factory = mockStatic(InfluxClientProxy.class)) {
            factory.when(() -> InfluxClientProxy.newDbClient(properties)).thenReturn(newDriver);
            factory.when(() -> InfluxClientProxy.newClient(newDriver, properties)).thenCallRealMethod();

            assertThrows(NullPointerException.class, () -> proxy.refreshClient(properties));
            assertSame(oldClient, proxy.client());
            verify(newDriver).close();
            verify(oldDriver, never()).close();
            verify(oldClient, never()).close();
        }
    }

    /** 验证完整创建并发布新实例后，才关闭旧客户端。 */
    @Test
    void successfulRefreshPublishesNewClientBeforeClosingOldClient() {
        InfluxDBClientImpl oldDriver = mock(InfluxDBClientImpl.class);
        InfluxDBClientImpl newDriver = mock(InfluxDBClientImpl.class);
        InfluxClient oldClient = mock(InfluxClient.class);
        InfluxClient newClient = mock(InfluxClient.class);
        InfluxClientProxy proxy = new InfluxClientProxy(oldDriver, oldClient);
        InfluxDbProperties properties = new InfluxDbProperties();
        when(newDriver.getServerVersion()).thenReturn("new");
        doAnswer(invocation -> {
            assertSame(newClient, proxy.client());
            assertEquals("new", proxy.getServerVersion());
            return null;
        }).when(oldClient).close();

        try (MockedStatic<InfluxClientProxy> factory = mockStatic(InfluxClientProxy.class)) {
            factory.when(() -> InfluxClientProxy.newDbClient(properties)).thenAnswer(invocation -> {
                verify(oldClient, never()).close();
                return newDriver;
            });
            factory.when(() -> InfluxClientProxy.newClient(newDriver, properties)).thenAnswer(invocation -> {
                verify(oldClient, never()).close();
                return newClient;
            });

            proxy.refreshClient(properties);

            verify(oldClient).close();
            verify(newClient, never()).close();
            assertSame(newClient, proxy.client());
        }
    }

    /** 验证旧连接关闭失败时，已发布的新连接仍保持可用。 */
    @Test
    void oldCloseFailureKeepsNewClientPublished() {
        InfluxDBClientImpl newDriver = mock(InfluxDBClientImpl.class);
        InfluxClient oldClient = mock(InfluxClient.class);
        InfluxClient newClient = mock(InfluxClient.class);
        InfluxClientProxy proxy = new InfluxClientProxy(mock(InfluxDBClientImpl.class), oldClient);
        InfluxDbProperties properties = new InfluxDbProperties();
        doThrow(new IllegalStateException("模拟旧客户端关闭失败")).when(oldClient).close();

        try (MockedStatic<InfluxClientProxy> factory = mockStatic(InfluxClientProxy.class)) {
            factory.when(() -> InfluxClientProxy.newDbClient(properties)).thenReturn(newDriver);
            factory.when(() -> InfluxClientProxy.newClient(newDriver, properties)).thenReturn(newClient);

            assertDoesNotThrow(() -> proxy.refreshClient(properties));
            assertSame(newClient, proxy.client());
        }
    }

    /** 验证真实初始化失败日志中出现掩码令牌，同时配置中的真实值可正常读取。 */
    @Test
    void initializationFailureMasksTokenInLog() {
        Logger logger = (Logger) LoggerFactory.getLogger(InfluxClientProxy.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            InfluxDbProperties properties = new InfluxDbProperties();
            properties.setToken(TEST_TOKEN);
            properties.setUrl(null);

            assertNull(InfluxClientProxy.instance(properties));
            List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
            assertTrue(messages.stream().anyMatch(message -> message.contains(MASKED_TOKEN)));
            assertFalse(messages.stream().anyMatch(message -> message.contains(TEST_TOKEN)));
            assertEquals(TEST_TOKEN, properties.getToken());
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }
    }
}
