<div align="center">

# Influx Client 4j

对 InfluxDB 3 Java SDK 的轻量封装：用对象写入数据，用链式 SQL 查询，再把结果映射回 POJO。

![Java](https://img.shields.io/badge/Java-21-orange)
![InfluxDB](https://img.shields.io/badge/InfluxDB-3.x-blue)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.5%20%28optional%29-6DB33F)
![Version](https://img.shields.io/badge/version-1.6.3.3--SNAPSHOT-lightgrey)

</div>

---

官方的 Java SDK `influxdb3-java` 只提供最底层的读写接口：写入要自己拼 `Point`，查询要自己写 SQL 再按下标取值，结果还得逐列转成对象。本库把这些重复劳动收敛成三件事：

- **写入靠对象**：继承 `AbstractActionInfluxObj`，声明 measurement、tag、field，直接写库，不用手搓 line protocol。
- **查询靠链式调用**：`InfluxQueryWrapper` 负责选列、拼条件、排序分页，条件值以占位符绑定。
- **结果靠映射**：查询结果按列名自动映射成 POJO、`Map`、`InfluxResult` 或 `InfluxPage`。

Spring Boot 项目引入后只需一段 `influx.*` 配置；非 Spring 项目也可直接 `new`，Spring 依赖是 `optional` 的，不会被传递引入。

## 特性

- 对象映射：标签 / 字段显式声明，写入前做交叉校验，避免同名标签与字段。
- 链式查询：`where()` 组合条件，`modify()` 排序分页，聚合通过 `AggSql` 构造。
- 结果映射：蛇形转驼峰、别名、父类字段、未匹配列兜底、自定义 `TypeHandler`。
- 写入缓存：内存队列按条数或时间间隔刷盘，支持分批并行写入。
- 分页查询：自动统计总数并回填 `InfluxPage`。
- LVC 支持：创建、删除最后值缓存（Last Value Cache），查询可直接走缓存表。
- 配置热更新：代理对象支持按新配置重建客户端。

## 环境要求

| 依赖 | 版本 | 说明 |
| --- | --- | --- |
| JDK | 21+ | 源码与字节码目标均为 21 |
| InfluxDB | 3.x | 依赖 `influxdb3-java` 的 SQL 查询接口，不适用于 1.x / 2.x |
| `influxdb3-java` | 1.7.0 | 由本库传递引入 |
| Spring Boot | 3.3.5（可选） | 仅自动配置与 `@ConfigurationProperties` 需要 |

> 本库只面向 **InfluxDB 3**。InfluxDB 3 默认 HTTP 端口是 `8181`；`8086` 是旧版本端口，除非服务端做过映射，否则连不上。

## 安装

尚未发布到 Maven Central。从源码安装到本地仓库：

```bash
git clone https://github.com/RylinWolf/influx-client.git
cd influx-client
mvn -DskipTests install
```

然后引入依赖：

```xml
<dependency>
    <groupId>com.wolfhouse</groupId>
    <artifactId>influxclient4j</artifactId>
    <version>1.6.3.3-SNAPSHOT</version>
</dependency>
```

`pom.xml` 里预留了私有仓库地址（`distributionManagement`），团队内使用可改成自己的 Maven 仓库并执行 `mvn deploy`。

## 快速开始

### Spring Boot

```yaml
influx:
  url: http://localhost:8181
  database: my_database
  token: ${INFLUX_TOKEN}   # 不写时默认读取环境变量 INFLUX_TOKEN
```

满足以下条件时自动装配生效：classpath 存在 `InfluxClient` 与 `InfluxDBClient`，且配置了 `influx.url`。

```java
@Autowired
private InfluxClientProxy influxClient;
```

注意注入类型是 **`InfluxClientProxy`**，不是 `InfluxClient`。自动配置只注册代理与 `InfluxLvcService`；直接注入 `InfluxClient` 会找不到 Bean。

token 为空时工厂返回 `null`，Spring 上下文会启动失败，请确认环境变量已就绪。需要自定义客户端时，自行声明 `InfluxClientProxy` 或 `InfluxLvcService` Bean 即可，两者均为 `@ConditionalOnMissingBean`。

### 非 Spring

脱离 Spring 也有两种建法：只需要读写时直接 `new InfluxClient`；想复用 `influx.*` 配置、配置热刷新与 LVC 时，用 `InfluxClientProxy`。

**方式一：直接使用 `InfluxClient`**

```java
InfluxDBClient dbClient = InfluxDBClient.getInstance(
        "http://localhost:8181",
        System.getenv("INFLUX_TOKEN").toCharArray(),
        "my_database");

InfluxClient influxClient = new InfluxClient(dbClient);
influxClient.setCacheBound(1000);                          // 缓存条数上限
influxClient.setCacheFlushInterval(Duration.ofSeconds(5)); // 缓存刷新间隔

// ... 读写 ...

// 使用结束后释放连接；close() 会先尝试把写入缓存刷盘
influxClient.close();
```

**方式二：使用 `InfluxClientProxy`**

代理会把 `InfluxDbProperties` 中的缓存参数应用到客户端，并在 `refreshClient` 时按新配置重建连接，适合配置来自文件或注册中心的场景。`InfluxClientProxy` 暴露的读写方法与 `InfluxClient` 一致。

```java
InfluxDbProperties properties = new InfluxDbProperties();
properties.setUrl("http://localhost:8181");
properties.setDatabase("my_database");
properties.setToken(System.getenv("INFLUX_TOKEN"));
properties.setCacheBound(1000L);
properties.setCacheFlushInterval(60_000L);   // 毫秒

InfluxClientProxy proxy = InfluxClientProxy.instance(properties);
if (proxy == null) {
    throw new IllegalStateException("Influx 客户端初始化失败，请检查 token 与地址");
}

proxy.insert(data);
List<SensorRecord> records = proxy.queryMap(wrapper, SensorRecord.class);
System.out.println("服务端版本: " + proxy.getServerVersion());

// 配置变更后重建连接，旧连接会被关闭
proxy.refreshClient(properties);

// 释放缓存与底层连接
proxy.close();
```

`instance` 在 token 为空或初始化异常时返回 `null`，必须判空。需要自行装配底层实例时，可以用 `newDbClient(properties)` 与 `newClient(dbClient, properties)` 两个静态方法，再构造代理；需要拿到原始的 `InfluxClient` 时调用 `proxy.client()`。

LVC 同样可以脱离 Spring 使用：

```java
InfluxLvcService lvcService = new InfluxLvcService(properties, proxy);
lvcService.createLvc("t_sensor_record", List.of("sensor_id"));
```

非 Spring 环境不要加载 `InfluxClientAutoConfiguration`。核心日志使用 SLF4J，需自行提供 Logback / Log4j2 实现。

缓存默认值因入口而异：直接 `new InfluxClient` 时缓存关闭、刷新间隔 1 秒；经代理创建时取自 `InfluxDbProperties`，其中 `cacheFlushInterval` 默认 60000 毫秒。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 映射对象 | 继承 `AbstractActionInfluxObj`，承载 measurement、tag、field、时间戳 |
| 结果对象 | 继承 `AbstractBaseInfluxObj`，用于承接查询结果 |
| 查询链 | `InfluxQueryWrapper`，维护查询列、条件与修饰符，最终 `build()` 出 SQL |
| 条件构造器 | `InfluxConditionWrapper`，由 `where()` 取得，负责比较与嵌套条件 |
| 修饰符构造器 | `InfluxModifiersWrapper`，由 `modify()` 取得，负责排序、分组、分页 |
| 结果映射器 | `InfluxObjMapper`，把 `Stream<Object[]>` 转成对象 |

一个表可以对应多种映射对象，但一个映射对象只绑定一个表。measurement 目前绑定在**实例**上，而非类上。

## 写入数据

### 定义映射对象

```java
public class SensorData extends AbstractActionInfluxObj {
    public SensorData() {
        super("t_sensor_record");
        // 声明列结构，便于 from(obj) 校验查询列
        addTags(InfluxTags.from("sensor_id", null).add("type", null));
        addFields(InfluxFields.from("temperature", null).add("humidity", null));
    }
}
```

### 写入

```java
SensorData data = new SensorData();
data.addTag("sensor_id", "SN-001")
    .addTag("type", "DHT11")
    .addField("temperature", 25.5)
    .addField("humidity", 60.2)
    .refreshTimestamp();   // 默认时间戳是构造时刻，这里刷新为当前时间

influxClient.insert(data);
```

| 方法 | 行为 |
| --- | --- |
| `insert(obj)` | 立即写入一条 |
| `insertAll(objs)` | 一次性写入整个集合 |
| `insertBatch(objs, batchSize)` | 分批并行写入，并等待全部完成 |
| `insertCache(obj)` / `insertCache(objs)` | 写入内存缓存；达到上限或定时任务触发时刷盘 |
| `handleCache()` | 手动刷盘，返回 `CompletableFuture` |
| `enableCache()` | 显式启用缓存（`insertCache` 会自动调用） |

### 校验与类型规则

| 规则 | 行为 |
| --- | --- |
| tag 值 | 必须是 `String` |
| field 值 | `Object`，支持常见数值、字符串、布尔等 |
| 标签 / 字段重名 | 抛出 `DuplicateFieldTagException` |
| 空对象 | tag 与 field 同时为空时，写入前抛 `InfluxObjValidException` |
| measurement | 为空则构建 `Point` 失败，务必在构造器中传入 |

> **数值类型一旦写入即被锁定**：某列首次写入整数 `0`，之后写 `0.01` 会因 line protocol 类型不兼容而失败。需要小数请从一开始就用 `Double`。

映射对象本身不是线程安全的，不要跨线程修改同一实例。

## 查询数据

### 创建查询链

| 入口 | 用途 |
| --- | --- |
| `from(obj)` | 用对象上的 measurement / tag / field 做约束；查询未声明的列会抛 `NoSuchTagOrFieldException` |
| `fromBuild(obj)` | 在 `from` 基础上立即选中该对象的全部 tag 与 field |
| `fromBuildSql(obj)` | 同上，并直接返回 SQL 字符串 |
| `create("measurement")` | 匿名查询，不校验列名 |
| `system("measurement")` | 匿名查询且不加反引号，用于系统表 |

### 选列、条件与修饰符

```java
InfluxQueryWrapper<SensorData> wrapper = InfluxQueryWrapper.from(new SensorData())
        .select("temperature", "humidity", "sensor_id")
        .withTime(true);          // 默认 true，结果会多出 time 列

wrapper.where()
       .eq("sensor_id", "SN-001")
       .gt("temperature", 20)
       .lt("temperature", 30)
       .or(w -> w.eq("type", "DHT22"));

wrapper.modify()
       .orderBy(true, "time")     // true 为降序，false 或不传方向为升序
       .limit(10, 0);
```

条件方法普遍提供 `boolean condition` 重载，条件为 `false` 时跳过该子句，便于拼装动态查询。连续的基本条件会自动用 `AND` 连接。

| 方法 | 说明 |
| --- | --- |
| `eq` / `ne` / `gt` / `ge` / `lt` / `le` | 比较条件 |
| `in(column, values)` | `IN` 条件，空集合会被忽略 |
| `duration(start, end)` | 时间区间，作用于 `time` 列 |
| `and` / `or` | 嵌套条件，接收 `Consumer` |
| `orderBy` / `groupBy` / `limit` / `offset` | 查询修饰符 |
| `having` | 聚合后的过滤，**必须先调用过 `where()`** |

多列 `orderBy`、`groupBy` 都按传入顺序生成 SQL；`groupBy` 的重复列名会被自动去重。

聚合辅助目前只有最大值与最小值：

```java
wrapper.withTime(false)
       .select("sensor_id")
       .selectO(AggSql.max("temperature").as("max_temperature"))
       .modify()
       .groupBy("sensor_id");
```

别名会成为结果列名，映射时按别名匹配字段；也可以直接写 `select("temperature as temp")`。

条件值以 `$param_n` 占位符交给驱动绑定，不直接拼接；但列名与表名仍会拼进 SQL（默认用反引号包裹），不要把不可信输入当作列名传入。

### 执行与结果形态

```java
List<SensorRecord> records = influxClient.queryMap(wrapper, SensorRecord.class);
List<Map<String, Object>> rows = influxClient.queryMap(wrapper);
InfluxResult result = influxClient.queryResult(wrapper);
Stream<Object[]> raw = influxClient.query(wrapper);
```

`query`、`queryMap`、`queryResult` 默认先执行一次 `count`，结果为 0 时直接返回空集，因此一次查询至少两次往返。确认不需要时传 `countCheck = false`：

```java
List<SensorRecord> records = influxClient.queryMap(wrapper, SensorRecord.class, false);
```

`query(sql, parameters)` 可直接执行原生 SQL：不做列校验，也不做计数短路，适合复杂查询。

## 结果映射

结果对象继承 `AbstractBaseInfluxObj`，必须有无参构造器：

```java
public class SensorRecord extends AbstractBaseInfluxObj {
    private String sensorId;     // 对应 sensor_id
    private Double temperature;  // 对应 temperature
    private Double humidity;
}
```

映射规则：

- 先按 `sensor_id` → `sensorId` 转驼峰查找，找不到再用原始列名查找，并沿父类逐层查找。
- 找不到且未使用 `@OtherColumns` 时跳过该列，仅记录警告。
- 字段已有非空初值时默认**不覆盖**；字段或类上标注 `@OverrideColumn` 后允许覆盖。
- `time` 列默认用 `InstantTypeHandler` 转为 `Instant`。

| 注解 | 作用 |
| --- | --- |
| `@OtherColumns` | 标在已初始化的 `Map<String, Object>` 上，接收未匹配的列；父类中的该字段不能是 `private`，`excludePatterns` 为正则排除 |
| `@OverrideColumn` | 允许查询值覆盖字段初值；`ignoreNull = true`（默认）时 null 不覆盖 |
| `@InfluxTypeHandler` | 指定字段使用自定义 `TypeHandler`，`subTypeHandler` 用于集合元素转换 |

内置处理器：`InstantTypeHandler`、`StringTypeHandler`、`TextTypeHandler`、`IntTypeHandler`、`SetTypeHandler`。

## 分页

```java
InfluxPage<SensorRecord> page = influxClient.pagination(wrapper, SensorRecord.class, 1, 10);

long total = page.total();
List<SensorRecord> current = page.records();
```

`InfluxPage` 由 Lombok `@Accessors(fluent = true)` 生成，读取请用 `page.total()`、`page.records()`、`page.pageNum()`，没有 `getTotal()` 之类的方法。

两个注意点：

- `pagination` 会**改写传入 wrapper** 的 `limit` / `offset`，不要拿同一个 wrapper 再分页第二次。
- `pageNum` 与 `pageSize` 应大于 0。源码中的断言只在启用 `-ea` 时生效；方法注释写“传 0 表示不限制”，与断言并不一致，不要依赖 0。

## 最近记录与 LVC

### 先查最近一条：`addRecent` / `addQueryRecent`

`addRecent(wrapper, "sensor_id")` 只选出分组列和 `max(time) as recent_time`，拿到的是“每组最近的时间”，不是完整行。

要拿每组最近一行的全部列，用 `addQueryRecent`：它先查各组最近时间，再拼等值条件回表。分组多、数据量大时，这次回表的开销会很明显——这正是 LVC 要解决的问题。

### LVC 是什么

Last Value Cache（LVC，最后值缓存）是 InfluxDB 3 内置的**服务端缓存表**：按指定的一组列分组，自动保存每组**最近写入的若干条**记录。它由数据库在写入时同步维护，不需要额外的定时任务、物化视图或应用侧代码。

它解决的问题很具体：**“每台设备 / 每个序列的最新状态”**。没有 LVC 时，这类查询要 `GROUP BY device_id` 配 `max(time)`，再回表关联，代价随历史数据量增长；有 LVC 后，查询退化成读一张小表。

创建缓存时由几个参数决定它的形态：

| 参数 | 含义 | 本库默认 |
| --- | --- | --- |
| key columns | 分组键，通常是设备 ID、序列等标签列 | 必填 |
| value columns | 需要缓存的列；省略时缓存所有非 key 列 | 省略 |
| count | 每组保留的版本数，`1` 表示只留最新一条 | `1` |
| ttl | 缓存的存活时间，过期后失效 | 4 小时 |

缓存表用 `last_cache('表名', '缓存名')` 引用，可以像普通表一样查询，也支持在它上面继续加条件、排序和分页。

> LVC 属于数据库级配置，创建和删除走管理接口，因此需要具备相应权限的 token；查询缓存表则与普通查询无异。官方说明见 [Create a Last Value Cache](https://docs.influxdata.com/influxdb3/core/admin/last-value-cache/create/)。

### 在代码里使用

```java
@Autowired
private InfluxLvcService lvcService;

// 按 sensor_id 分组，缓存每组最新一条
lvcService.createLvc("t_sensor_record", List.of("sensor_id"));

// 查看当前数据库已有的缓存
List<LastCaches> caches = lvcService.lvcTables();
```

`createLvc` 调用 `POST /api/v3/configure/last_cache`。未指定时缓存名与 measurement 相同，版本数 1，TTL 4 小时；创建失败返回 `false` 并记日志，不抛检查型异常。需要指定缓存名或 TTL 时使用重载：

```java
lvcService.createLvc("t_sensor_record", "sensor_last", List.of("sensor_id"), 3600L);
```

删除用 `deleteLvc(measurement, name)`。

已有 LVC 时，可让查询直接走缓存表：

```java
InfluxQueryWrapper<SensorData> recent = influxClient.addQueryRecentByCache(wrapper, "t_sensor_record");
```

第二个参数是 LVC 名称；该方法会把查询来源改成 `last_cache(...)` 并去掉表名引号，条件、排序与分页仍然生效。

## 配置项

| 配置 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `influx.url` | `String` | 无（必填） | 服务地址，如 `http://localhost:8181` |
| `influx.database` | `String` | 无（必填） | 目标数据库 / bucket |
| `influx.token` | `String` | `INFLUX_TOKEN` 环境变量 | 访问令牌 |
| `influx.cache-bound` | `Long` | `1000` | 缓存条数上限 |
| `influx.cache-flush-interval` | `Long` | `60000` | 缓存刷新间隔，单位毫秒 |
| `influx.lvc-timeout` | `Duration` | `15s` | LVC 管理接口超时 |

只有配置了 `influx.url` 才会触发自动配置，缺少 `url` 时不会注册任何 Bean。`token` 或 `database` 为空会导致客户端创建失败并返回 `null`，启动阶段即报错，建议显式写全。

## 配置热更新

`InfluxClient` 创建后不会自己观察配置。要通过注册中心修改配置后立即生效，在使用方项目中监听配置变更，把新值写回 `InfluxDbProperties`，再刷新代理。

```java
@Component
@RequiredArgsConstructor
@Slf4j
public class InfluxClientManager {
    private final InfluxClientProxy client;
    private final InfluxDbProperties properties;
    private final InfluxClientConfig config;

    /** 刷新器，在 nacos 配置发生变更时更新 InfluxClient */
    @EventListener(EnvironmentChangeEvent.class)
    public synchronized void onRefresh(EnvironmentChangeEvent event) {
        Set<String> keys = event.getKeys();
        if (keys.contains(InfluxClientConfig.CACHE_BOUND_KEY)
                || keys.contains(InfluxClientConfig.CACHE_FLUSH_INTERVAL_KEY)) {
            log.info("[InfluxClientManager] 监听到配置变更: {}", keys);
            properties.setCacheBound(config.getCacheBound());
            properties.setCacheFlushInterval(config.getCacheFlushInterval());
            // 刷新客户端
            this.client.refreshClient(properties);
        }
    }
}
```

应当使用 `InfluxClientProxy`，不要直接注入 `InfluxClient`，否则更新配置不会作用到正在使用的客户端。刷新会关闭旧连接，并按新配置重建客户端。

`InfluxLvcService` 依赖同一个代理与 `InfluxDbProperties` 实例，因此刷新后 LVC 的查询与管理请求都会自动使用新配置，无需重建 Bean。

## 已知限制

- measurement 绑定在实例上，暂不支持类级注解绑定。
- 没有通用聚合函数构造器，目前只封装 `max` / `min`。
- 不封装连接、子查询或任意 SQL 片段；复杂语句请用 `query(sql, parameters)`。
- 除 LVC 配置接口外，不提供库、表等 schema 管理能力。
- 查询链、条件构造器与映射对象均不支持并发复用。

## 常见问题

### 启动报 `NoClassDefFoundError: com/google/protobuf/RuntimeVersion`

`influxdb3-java` 1.7.0 依赖 `protobuf-java` **4.33.1**。运行时若加载了更低版本，Protobuf 会拒绝启动。请在自己的依赖中把 `protobuf-java` 对齐到 4.33.1，或用 `mvn dependency:tree` 找出并排除旧版本。

### 写入报 `parsing for line protocol failed`

先确认 measurement、tag、field、时间戳都合法，再检查数值列是否发生了类型变化（见上文“数值类型一旦写入即被锁定”）。同一 measurement 的列数也受服务端限制。可把调试日志里的 line protocol 用官方接口重放，拿到服务端原始报错。

### 查询结果字段全是 `null`

依次检查：查询列名经蛇形转驼峰后能否命中字段；是否使用了别名而结果类字段名没跟着改；结果类是否有无参构造器；字段是否因声明时赋了初值而未被覆盖（需要 `@OverrideColumn`）。

### 注入 `InfluxClient` 报找不到 Bean

自动配置注册的是 `InfluxClientProxy`，请改为注入代理；也可自行声明 `InfluxClient` Bean。

## 构建与测试

```bash
mvn -DskipTests package   # 打包 jar、sources、javadoc
mvn -DskipTests install   # 安装到本地仓库，供其他项目依赖
```

集成测试需要本地运行 InfluxDB（`localhost:8181`）并提供 token，仓库通过 `.gitignore` 排除了 `src/test/`，克隆后默认不含测试代码。

## 许可证

仓库暂未包含 `LICENSE` 文件。在补充许可证之前，默认保留所有权利；
