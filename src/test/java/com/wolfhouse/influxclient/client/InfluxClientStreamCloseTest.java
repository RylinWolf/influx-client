package com.wolfhouse.influxclient.client;

import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.exception.InfluxClientQueryException;
import com.wolfhouse.influxclient.pojo.AbstractBaseInfluxObj;
import com.wolfhouse.influxclient.properties.InfluxDbProperties;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * 验证库内部消费的 SDK 查询流在正常和异常退出时均被关闭。
 *
 * @author Rylin Wolf
 */
class InfluxClientStreamCloseTest {
    /** 验证计数查询关闭底层查询流。 */
    @Test
    void countClosesQueryStream() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation ->
                Stream.<Object[]>of(new Object[]{2L}).onClose(closes::incrementAndGet));
        InfluxClient client = new InfluxClient(driver);

        assertEquals(2L, client.count(InfluxQueryWrapper.create("sensor").select("value")));
        assertEquals(1, closes.get());
    }

    /** 验证查询为空时也会关闭底层流。 */
    @Test
    void emptyMapQueryClosesStream() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation -> Stream.<Object[]>empty().onClose(closes::incrementAndGet));
        InfluxClient client = new InfluxClient(driver);

        assertTrue(client.queryMap(InfluxQueryWrapper.create("sensor").select("value"), false).isEmpty());
        assertEquals(1, closes.get());
    }

    /** 验证 POJO 映射成功后关闭底层流。 */
    @Test
    void objectQueryClosesStreamAfterMapping() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation ->
                Stream.<Object[]>of(new Object[]{"incoming"}).onClose(closes::incrementAndGet));
        InfluxClient client = new InfluxClient(driver);

        var rows = client.queryMap(InfluxQueryWrapper.create("sensor").select("value").withTime(false), Row.class, false);

        assertEquals("incoming", rows.getFirst().value);
        assertEquals(1, closes.get());
    }

    /** 验证流消费过程抛异常时仍会关闭流。 */
    @Test
    void failedStreamConsumptionStillClosesStream() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation -> Stream.<Object[]>generate(() -> {
            throw new IllegalStateException("模拟查询流读取失败");
        }).onClose(closes::incrementAndGet));
        InfluxClient client = new InfluxClient(driver);

        assertThrows(InfluxClientQueryException.class,
                () -> client.queryMap(InfluxQueryWrapper.create("sensor").select("value"), false));
        assertEquals(1, closes.get());
    }

    /** 验证 POJO 构造失败时仍会关闭底层流。 */
    @Test
    void failedObjectMappingStillClosesStream() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation ->
                Stream.<Object[]>of(new Object[]{"incoming"}).onClose(closes::incrementAndGet));
        InfluxClient client = new InfluxClient(driver);

        assertThrows(InfluxClientQueryException.class,
                () -> client.queryMap(InfluxQueryWrapper.create("sensor").select("value").withTime(false), BrokenRow.class, false));
        assertEquals(1, closes.get());
    }

    /** 验证结果封装查询关闭计数和数据查询各自的流。 */
    @Test
    void resultQueryClosesCountAndDataStreams() {
        InfluxDBClient driver = mock(InfluxDBClient.class);
        AtomicInteger closes = new AtomicInteger();
        when(driver.query(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.startsWith("select count")) {
                return Stream.<Object[]>of(new Object[]{1L}).onClose(closes::incrementAndGet);
            }
            return Stream.<Object[]>of(new Object[]{"incoming"}).onClose(closes::incrementAndGet);
        });
        InfluxClient client = new InfluxClient(driver);

        var result = client.queryResult(InfluxQueryWrapper.create("sensor").select("value").withTime(false));

        assertEquals("incoming", result.row(0).get("value"));
        assertEquals(2, closes.get());
    }

    /** 验证 LVC 元数据查询消费后关闭底层流。 */
    @Test
    void lvcMetadataQueryClosesStream() {
        InfluxClientProxy proxy = mock(InfluxClientProxy.class);
        AtomicInteger closes = new AtomicInteger();
        when(proxy.query(any(InfluxQueryWrapper.class))).thenAnswer(invocation ->
                Stream.<Object[]>empty().onClose(closes::incrementAndGet));
        InfluxLvcService service = new InfluxLvcService(new InfluxDbProperties(), proxy);

        assertTrue(service.lvcTables().isEmpty());
        assertEquals(1, closes.get());
    }

    /**
     * 用于正常 POJO 映射的结果对象。
     *
     * @author Rylin Wolf
     */
    public static class Row extends AbstractBaseInfluxObj {
        /** 待注入的查询字段 */
        public String value;
    }

    /**
     * 用于验证映射失败资源释放的结果对象。
     *
     * @author Rylin Wolf
     */
    public static class BrokenRow extends AbstractBaseInfluxObj {
        /** 构造结果对象时模拟失败。 */
        public BrokenRow() {
            throw new IllegalStateException("模拟结果对象构造失败");
        }
    }
}
