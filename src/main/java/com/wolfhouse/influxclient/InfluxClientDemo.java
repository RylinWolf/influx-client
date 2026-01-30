package com.wolfhouse.influxclient;

import com.wolfhouse.influxclient.client.InfluxClient;
import com.wolfhouse.influxclient.core.InfluxModifiersWrapper;
import com.wolfhouse.influxclient.core.InfluxQueryWrapper;
import com.wolfhouse.influxclient.pojo.AbstractActionInfluxObj;
import com.wolfhouse.influxclient.pojo.InfluxFields;
import com.wolfhouse.influxclient.pojo.InfluxTags;
import com.wolfhouse.influxclient.test.SampleActionInfluxObj;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @author Rylin Wolf
 */
@SpringBootApplication
public class InfluxClientDemo {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(InfluxClientDemo.class, args);

        InfluxClient client = context.getBean(InfluxClient.class);
        System.out.println(client.client.getServerVersion());


        //var obj = testInfluxObj();
        //testQueryWrapper(obj);

        //client.query(testGetSql(obj));

        //System.out.println(testQueryCondition(obj));

        //client.queryAndShow(testGetQueryWrapper(obj));

        //testQuery(client).forEach(row -> {
        //    System.out.println("=======================");
        //    for (Object o : row) {
        //        System.out.printf("%s ", o);
        //    }
        //    System.out.println();
        //});

        //SimpleActionInfluxObj obj = new SimpleActionInfluxObj();
        //obj.addTags(InfluxTags.from("sensor_id", null)
        //                      .add("sensor_type", null));
        //obj.addFields(InfluxFields.from("o2", null)
        //                          .add("co2", null)
        //                          .add("temperature", null));
        //
        //InfluxQueryWrapper<SimpleActionInfluxObj> wrapper = InfluxQueryWrapper.fromBuild(obj)
        //                                                                      .where()
        //                                                                      .and((w) -> {
        //                                                                          w.eq("sensor_type", "气体温度传感器")
        //                                                                           .gt("o2", 30)
        //                                                                           .ne("sensor_id", "123456789");
        //                                                                      })
        //                                                                      .or(w -> {
        //                                                                          w.lt("temperature", 30)
        //                                                                           .eq("sensor_id", "123456789");
        //                                                                      }).parent();
        //Stream<Object[]>      result = client.query(wrapper);
        //List<SimpleInfluxObj> objs   = InfluxObjMapper.mapAll(result, SimpleInfluxObj.class, wrapper);
        //objs.forEach(System.out::println);


        //testModifierWrapper();


        //InfluxQueryWrapper<SampleActionInfluxObj> wrapper = testModifierWrapperBind();
        //ArrayList<SampleInfluxObj>                objs    = new ArrayList<>(client.queryMap(wrapper, SampleInfluxObj.class));
        //for (SampleInfluxObj obj : objs) {
        //    System.out.println(obj);
        //}


        //System.out.println(client.query(wrapper));

        //InfluxQueryWrapper<SampleActionInfluxObj> wrapper = testPagination();
        //InfluxPage<SampleInfluxObj> page = client.pagination(wrapper, SampleInfluxObj.class,
        //        3, 3);
        //System.out.println(page);

        //InfluxQueryWrapper<SampleActionInfluxObj> wrapper = testPagination();
        //List<Map<String, Object>>                 maps    = client.queryMap(wrapper);
        //System.out.println(maps);
        //InfluxResult result = client.queryResult(wrapper);
        //System.out.println(result);

    }

    public static SampleActionInfluxObj testInfluxObj() {
        SampleActionInfluxObj obj = new SampleActionInfluxObj();
        obj.addTags(InfluxTags.from("location", "Recent York")
                              .add("sensor", "temperature")
                              .add("sensor_type", "ambient")
                              .add("sensor_id", "123456789"));

        obj.addField("value", 211.1);
        obj.addField("unit", "celsius");
        return obj;
    }

    public static void testQueryWrapper(AbstractActionInfluxObj obj) {
        var wrapperSql = InfluxQueryWrapper.from(obj)
                                           .select("location",
                                                   "sensor",
                                                   "value")
                                           .build();

        var wrapperObjSql = InfluxQueryWrapper.from(obj)
                                              .selectSelfTag()
                                              .selectSelfField()
                                              .build();

        var wrapperObjBuildSql = InfluxQueryWrapper.fromBuildSql(obj);

        var wrapperLambda = InfluxQueryWrapper.create()
                                              .measurement("temperature")
                                              .select("location", "value")
                                              .build();

        System.out.println(wrapperSql);
        System.out.println(wrapperObjSql);
        System.out.println(wrapperObjBuildSql);
        System.out.println(wrapperLambda);
    }

    public static String testGetSql(AbstractActionInfluxObj obj) {
        return InfluxQueryWrapper.from(obj)
                                 .selectSelfTag()
                                 .select("value")
                                 .withTime(false)
                                 .build();
    }

    public static String testQueryCondition(AbstractActionInfluxObj obj) {
        return InfluxQueryWrapper.from(obj)
                                 .selectSelfTag()
                                 .where()
                                 .lt("time", Instant.now())
                                 .and((w) -> w.gt("value", 200)
                                              .or((o) -> o.le("value", 30)))
                                 .and((w) -> w.eq("sensor", "ambient"))
                                 .build();
    }

    public static InfluxQueryWrapper<?> testGetQueryWrapper(AbstractActionInfluxObj obj) {
        return InfluxQueryWrapper.from(obj)
                                 .selectSelfTag()
                                 .select("value")
                                 .where()
                                 .lt("time", Instant.now().toString())
                                 .and((w) -> w.gt("value", 100)
                                              .or((o) -> o.le("value", 50)))
                                 .parent();
    }

    public static void testInsert(InfluxClient client) {
        List<AbstractActionInfluxObj> objs = new ArrayList<>(50);
        for (int i = 0; i < 50; i++) {
            SampleActionInfluxObj obj = new SampleActionInfluxObj();
            obj.addTags(InfluxTags.from("sensor_id", "61262422222")
                                  .add("sensor_type", "气体温度传感器"));
            obj.addFields(InfluxFields.from("o2", new Random().nextDouble(50))
                                      .add("co2", new Random().nextDouble(50))
                                      .add("temperature", new Random().nextDouble(100)));
            objs.add(obj);
        }
        client.insertAll(objs);
    }

    public static Stream<Object[]> testQuery(InfluxClient client) {
        SampleActionInfluxObj obj = new SampleActionInfluxObj();
        obj.addTags(InfluxTags.from("sensor_id", null)
                              .add("sensor_type", null));
        obj.addFields(InfluxFields.from("o2", null)
                                  .add("co2", null)
                                  .add("temperature", null));

        return client.query(
                InfluxQueryWrapper.fromBuild(obj)
                                  .where()
                                  .and((w) -> {
                                      w.eq("sensor_type", "气体温度传感器")
                                       .gt("o2", 30)
                                       .ne("sensor_id", "123456789");
                                  })
                                  .or(w -> {
                                      w.lt("temperature", 30)
                                       .eq("sensor_id", "123456789");
                                  }).parent());
    }

    /**
     * 现在调用会导致 SQL 查询修饰符重复：QueryWrapper 集成了 ModifierWrapper
     */
    public static void testModifierWrapper() {
        SampleActionInfluxObj                         obj           = new SampleActionInfluxObj();
        InfluxQueryWrapper<SampleActionInfluxObj>     queryWrapper  = InfluxQueryWrapper.from(obj);
        InfluxModifiersWrapper<SampleActionInfluxObj> modifyWrapper = InfluxModifiersWrapper.create(queryWrapper);

        queryWrapper.selectSelfTag()
                    .where()
                    .eq("sensor_type", "气体温度传感器");

        modifyWrapper.limit(5, 2)
                     .orderBy(false, "sensor_id")
                     .groupBy("sensor_type");

        System.out.println(queryWrapper.build());
        System.out.println(modifyWrapper.toSql());
    }

    public static InfluxQueryWrapper<SampleActionInfluxObj> testModifierWrapperBind() {
        SampleActionInfluxObj                     obj          = new SampleActionInfluxObj();
        InfluxQueryWrapper<SampleActionInfluxObj> queryWrapper = InfluxQueryWrapper.from(obj);
        queryWrapper.selectSelfTag()
                    .select("o2")
                    .where()
                    .eq("sensor_type", "气体温度传感器")
                    .parent()
                    .modify()
                    .limit(10, 0)
                    .orderBy(true, "time")
                    .orderBy(false, "sensor_id");
        System.out.println(queryWrapper.build());
        return queryWrapper;
    }

    public static InfluxQueryWrapper<SampleActionInfluxObj> testPagination() {
        SampleActionInfluxObj                     obj     = new SampleActionInfluxObj();
        InfluxQueryWrapper<SampleActionInfluxObj> wrapper = InfluxQueryWrapper.from(obj);
        wrapper.selectSelfAll()
               .where()
               .eq("sensor_type", "气体温度传感器")
               .parent()
               .modify()
               .orderBy("o2");
        return wrapper;
    }
}

