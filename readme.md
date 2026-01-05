<div align="center">
  # Influx-Client
</div>

对 `InfluxDBClient` 进行二次封装，提供简单的 Query、Insert 操作。

**注意：InfluxDB 的 Java 客户端仅支持 添加、查询数据操作，不支持管理数据。**

# 项目介绍

`Influx-Client` 是一个基于 InfluxDB Java 客户端（V3）开发的轻量级二次封装框架。它旨在简化 Java 应用与 InfluxDB 时序数据库之间的交互，通过提供类似 MyBatis-Plus 的流式 API 和对象映射（ORM）机制，显著降低了开发者的上手难度和代码冗余。

### 核心特性

- **轻量级封装**：对 `InfluxDBClient` 进行深度简化，保留核心功能的同时屏蔽底层复杂性。
- **对象映射 (ORM)**：支持将 Java 对象直接映射为 InfluxDB 的度量（Measurement），自动处理标签（Tags）和字段（Fields）的区分与转换。
- **流式查询构建器**：引入 `InfluxQueryWrapper`，支持 SQL 风格的条件构造，提供 `where()`、`modify()` 等链式方法，轻松实现复杂查询。
- **Spring Boot 集成**：提供 Starter 组件，支持通过 `application.yml` 快速配置，并利用自动装配实现 `InfluxClient` 的零配置注入。
- **多结果处理**：内置结果映射器，可将查询结果自动转换为 POJO 列表、Map 集合、结果封装对象（InfluxResult）、分页对象（InfluxPage）或原生数据流。
- **安全性与校验**：在数据插入前自动进行标签与字段的交叉校验，防止非法的 Schema 写入。

### 适用场景

适用于需要频繁进行 InfluxDB 数据写入、按条件检索以及分页展示的 Java/Spring Boot 项目，特别是对于不希望编写原始 SQL/Flux 语句并追求开发效率的项目。


# 上手使用

本节展示如何使用 `Influx Client` 简化对 Influx DB 的操作。

## 初始化客户端

### Spring Boot 项目

1. 在 `application.yml` 或 `application.properties` 中添加配置：

```yaml
influx:
  url: http://localhost:8086 # 替换为你的 Influx 数据库地址
  database: my_bucket        # 替换为你的 Influx 数据库名
```

2. 在环境变量中配置 `INFLUX_TOKEN` 

3. 之后可以直接在代码中注入 `InfluxClient`：

```java
@Autowired
private InfluxClient influxClient;
```

### 非 Spring Boot 项目

虽然本项目提供了 Spring Boot Starter，但在非 Spring Boot 环境中同样可以轻松使用。

- **类加载安全性**：虽然 `InfluxClientAutoConfiguration` 类中使用了 Spring 的注解和类，但由于 `InfluxClient` 核心逻辑与之处于不同的包结构下（`client` 包 vs `autoconfigure` 包），在非 Spring 环境中，只要你不主动 `import` 自动配置类，JVM 就不会尝试加载它。因此，即便依赖库中包含这些 `.class` 文件，也不会在运行时抛出 `NoClassDefFoundError`。
- **依赖隔离**： `pom.xml` 中  Spring 相关依赖为 `<optional>true</optional>`。这意味着如果你是在一个非 Spring 项目中引入本框架，Maven 不会下载 Spring 相关的 jar 包，从而保持了项目的轻量性。

为了确保在脱离 Spring Boot 的环境下正常运行，请遵循以下改造步骤：

#### 1. 依赖配置

如果你不使用 Spring Boot，请确保你的 `pom.xml` 中排除了 Spring 相关的自动配置依赖，或者直接引入核心依赖所需的库。本项目已将 Spring Boot 相关依赖标记为 `optional`，因此在非 Spring 环境下，它们不会被强制引入。

你至少需要以下依赖：

```xml
<dependencies>
    <!-- InfluxDB V3 SDK -->
    <dependency>
        <groupId>com.influxdb</groupId>
        <artifactId>influxdb3-java</artifactId>
        <version>1.7.0</version>
    </dependency>
    <!-- Lombok (编译期插件) -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <version>1.18.30</version>
        <scope>provided</scope>
    </dependency>
</dependencies>
```

#### 2. 客户端初始化

在非 Spring 环境下，你需要手动创建 `InfluxDBClient` 实例，并将其注入到 `InfluxClient` 中。

```java
import com.influxdb.v3.client.InfluxDBClient;
import com.wolfhouse.influxclient.client.InfluxClient;

// 1. 创建原生的 InfluxDBClient (SDK 提供)
InfluxDBClient influxDBClient = InfluxDBClient.getInstance(
        "http://localhost:8086",         // 数据库地址
        "your-token".toCharArray(),       // 访问令牌
        "my_bucket"                      // 数据库名
);

// 2. 实例化 InfluxClient (本项目提供)
// 注意：不要使用 InfluxClientAutoConfiguration，它是为 Spring Boot 设计的
InfluxClient influxClient = new InfluxClient(influxDBClient);

// 3. 之后即可正常使用 influxClient 进行 insert/query 操作

// 4. 程序结束时记得关闭资源
// influxClient.close();
```

#### 3. 注意事项

- **配置管理**：由于脱离了 Spring Boot 的 `application.yml` 自动加载机制，你需要自行管理数据库连接配置（如从配置文件或环境变量读取）。
- **生命周期**：你需要手动管理 `InfluxClient` 的生命周期，确保在不再使用时调用 `close()` 方法以释放底层 HTTP 连接。
- **日志**：本项目核心逻辑使用 SLF4J 记录日志。在非 Spring 项目中，你需要自行引入日志实现（如 Logback 或 Log4j2）。

## 添加数据

要向 InfluxDB 添加数据，首先需要定义一个继承自 `AbstractActionInfluxObj` 的类来映射数据表。

### 1. 定义映射类

```java
@Data
public class SensorData extends AbstractActionInfluxObj {
    public SensorData() {
        super("t_sensor_record"); // 指定度量（表名）
        // 初始化字段和标签的结构（可选，更规范，也方便后续使用 Wrapper 校验）
        addTags(InfluxTags.from("sensor_id", null).add("type", null));
        addFields(InfluxFields.from("temperature", null).add("humidity", null));
    }
}
```

### 2. 插入数据

```java
SensorData data = new SensorData();
data.addTag("sensor_id", "SN-001")
    .addTag("type", "DHT11")
    .addField("temperature", 25.5)
    .addField("humidity", 60.2)
    .refreshTimestamp(); // 设置为当前时间

influxClient.insert(data);
```

## 查询数据

`Influx Client` 提供了 `InfluxQueryWrapper` 来构建 SQL 查询。

### 构建查询目标

可以使用 `select` 方法指定要查询的列，或使用快捷方法：

```java
InfluxQueryWrapper<SensorData> wrapper = InfluxQueryWrapper
  .from(new SensorData())
  .select("temperature","humidity") // 选择指定列
  // .selectAll()                  // 选择所有标签和字段
  // .selectSelfTag()                // 仅选择对象定义的标签
  .withTime(true);                   // 是否包含时间戳，默认为 true
```

### 构建查询条件

通过 `where()` 进入条件构建模式：

```java
wrapper.where()
    .eq("sensor_id","SN-001")
    .gt("temperature",20)
    .lt("temperature",30)
    .or(w ->w.eq("type","DHT22"));
```

支持嵌套条件：

```java
wrapper.where()
    .eq("sensor_id","SN-001")
    .and(w ->w.gt("temperature",20).and(w2 -> w2.lt("temperature",30)))
    .or(w ->w.eq("type","DHT22"));
```



### 构建查询修饰符

通过 `modify()` 构建排序、限制和偏移：

```java
wrapper.modify()
    .orderBy(false,"time") // 按照时间降序排列
    .limit(10,0);          // 限制 10 条，偏移 0
```

### 执行查询并映射结果

查询结果可以映射为 POJO（需继承 `AbstractBaseInfluxObj`）、结果封装对象（InfluxResult）或简单的 `Map` 列表：

```java
// 1. 映射为实体类列表 (最常用)
// 假设 SensorDataResult 继承了 AbstractBaseInfluxObj
Collection<SensorDataResult> results = influxClient.queryMap(wrapper, SensorDataResult.class);

// 2. 映射为 Map 列表 (键为列名，值为对应数据)
List<Map<String, Object>> list = influxClient.queryMap(wrapper);

// 3. 分页查询
// 返回 InfluxPage 对象，包含总记录数、当前页记录、页码等信息
InfluxPage<SensorDataResult> page = influxClient.pagination(wrapper, SensorDataResult.class, 1, 10);

// 4. 获取结果封装对象 (InfluxResult)
InfluxResult result = influxClient.queryResult(wrapper);

// 5. 获取底层结果流 (Stream<Object[]>)
Stream<Object[]> stream = influxClient.query(wrapper);

```

# 主要相关类

本项目的主要类可以分为以下类别：

- 通用类：包括配置类、工具类、处理器类等；
- 映射对象相关类：InfluxDB 数据表中的每一条记录，都可以映射到一个 Java 对象中，又称为 **领域对象（Domian）**。由于 InfluxDB
  表结构的特殊性（不预先设定架构，且列名分为标签、字段两种），对于每个表，单独的一个领域对象无法很好表示。以上，为很好地映射一个
  InfluxDB 记录，需要通过一系列类；
- 行为相关类：包括查询和插入行为，每种行为都有对应的相关类。

## 通用类

### 配置类

#### InfluxClientAutoConfiguration 自动装配类 < autoconfigure

用于 SpringBoot 的自动装配，为导入该包的项目提供默认的 InfluxDBClient（来自 Influx SDK）以及 InfluxClient（本项目核心类）实现。

该自动装配类引入了 `InfluxDbProperties` 配置（见下文），并在满足以下条件时才会配置：

- 项目中包含 InfluxClient、InfluxDBClient 类
- Spring 配置文件中配置了 InfluxDB 的相关配置（见 `resources.META-INF.additional-spring-configuration-metadata.json`）

自动装配类中提供的默认类均可被覆盖（ConditionalOnMissingBean）

#### InfluxDbProperties 配置属性类 < properties

封装了要连接到 InfluxDB 所需的配置信息。对应着 spring 配置文件中的字段，以 `influx` 为前缀。

包含以下属性

- token：访问令牌，从环境变量 `INFLUX_TOKEN` 读取
- url：服务器地址，对应配置文件 `influx.url`
- database：目标数据库（Bucket），对应配置文件 `influx.database`

### 类型处理器 < typehandler

#### InfluxTypeHandler 类型处理器注解

可用于字段，包含属性 `value`，用于指定实际要用的类型处理器 `TypeHandler`。

#### TypeHandler 类型处理器接口

类型处理器的接口，需要指定泛型。

声明了一个 `getResult` 方法，接收一个 Object 对象，返回指定的泛型对象。

#### InstantTypeHandler [Instant 时间戳] 类型处理器

项目默认提供的用于处理 Instant 实例的类型处理器。

通过自定义的时间戳工具 `TimeStampUtils` 将数字格式的字符串转为 Instant 对象。

### 工具类 < utils

#### TimeStampUtils 时间戳工具

封装了一系列方法，用于自动处理长整型，根据长度进行判断，并返回 Instant 对象。

## 映射对象相关类

### InfluxFields 字段映射

用于 `AbstractActionInfluxObj 抽象 Influx 操作对象`，表示 InfluxDB 的字段列。

使用基于链表的哈希表存储字段列，键 为字段名，值为该字段的值。维持插入顺序。

提供一系列初始化方法及操作字段数据的方法。

### InfluxTags 标签映射

用于 `AbstractActionInfluxObj 抽象 Influx 操作对象`，表示 InfluxDB 的标签列。

使用基于链表的哈希表存储标签列，键为标签名，值为该标签的值。维持插入顺序。

提供一系列初始化方法及操作标签数据的方法。

### AbstractBaseInfluxObj 抽象基础类

InfluxDB 的基础映射类，封装了两个基本属性：

- measurement：该对象对应的数据表
- time：数据点的时间戳

是 抽象操作类、结果类的父类。若要自定义返回结果映射，应继承该基础类。

### AbstractActionInfluxObj 抽象操作类

InfluxDB 的操作映射类，是写入、查询数据时使用的基类。包含两个基础属性：

- tags：InfluxTags，标签数据
- fields: InfluxFields，字段数据

提供了一系列操作标签、字段的方法。添加时会进行交叉校验，确保标签和字段没有交集。

### InfluxResult 结果封装类

InfluxDB 的查询结果封装，通过动态链表存放 Influx 结果行封装类。

#### InfluxRow 结果行封装类

使用映射表存放结果集中的一行数据。结果集中的一行数据就对应一个结果行封装对象。

### InfluxPage 结果分页类

封装分页查询的结果，包含以下属性

- pageNum：分页序号
- pageSize：当前页结果数量
- total：总数量
- records：结果集列表

## 行为相关类

### BaseSqlBuilder 基础 SQL 构造器模板

SQL 构造器模板，规范 SQL 构建的方法步骤。

包括以下默认方法：

- buildCondition：构建查询条件
- buildModifies：构建查询修饰符（order by、limit 等）
- build：执行构建步骤，依次进行验证、构建查询目标、构建查询表、构建条件/查询修饰符，最终返回构建好的 SQL 语句

包含以下抽象方法：

- buildTarget：构建查询目标（标签、字段）
- buildFromTable：构建查询目标表
- validate：执行构建前的字段验证

### InfluxConditionWrapper 条件 SQL 构造器

构建 Influx 查询 SQL 中条件部分的工具类，结合 `InfluxQueryWrapper Influx 查询构造器` 使用，实现根据条件查询。

提供了一系列构造查询条件的方法，支持链式调用，并基于 StringBuilder 实时保存查询条件。

包含以下属性：

- paramters：查询条件参数占位名与对应值的映射，键为自动构建的查询参数占位名，值为其应被替换的值。如：param_1 -> 1，则条件 SQL
  中 ${param_1} 最终会被替换为 1。通过此解决 SQL 注入的问题。
- targets：查询条件的目标字段，用于 `InfluxQueryWrapper` 在执行构建时的列名存在性检查
- builder：StringBuilder，实际的 SQL 语句
- paramIdx：查询条件参数计数器，用于生成查询条件占位参数名称，从 1 开始
- parent：当前构造器所属的父构造器（`InfluxQueryWrapper`）

包含以下方法：

- and：构建 and 条件，包含两个方法重载。若已有条件以 and 结尾，则不再构建 and 关键字
- or：构建 or 条件，包含两个方法重载。若已有条件以 or 结尾，则不再构建 or 关键字
- eq：构建等于条件
- ne：构建不等于条件
- lt：构建小于条件
- le：构建小于等于条件
- gt：构建大于条件
- ge：构建大于等于条件
- appendConditionAndMask：私有方法，在已有条件上追加一个带有指定操作符的条件，根据已存在的条件自动添加 `and` 连接符
- addColumnValueMapping：私有方法，将指定列名添加到查询目标集，并为其创建一个参数占位符名称， 添加占位符名称与指定值的映射，以便于后续进行参数注入
- mayDo：私有方法，使用匿名 wrapper 构建实际的分段条件，用于构建连接条件（AND/OR）体的内容。可指定连接符，从而构建包含连接符的条件体。该方法构建的内容是完整的条件。

### InfluxModifiersWrapper 修饰符 SQL 构建器

构建 Influx 查询 SQL 中查询修饰符部分的工具类，结合 `InfluxQueryWrapper` 使用，实现在 SQL 语句尾添加查询修饰符。

目前的查询修饰符支持 LIMIT/OFFSET，ORDER BY，暂不支持 GROUP BY（可添加，但查询 wrapper 暂时不支持聚合）。

包含以下属性：

- limit/offset：限制查询数量及偏移量
- orderBy：按照指定字段排序，可指定多个
- globalDesc：全局的排序规则，若指定排序字段时未传递规则参数，则使用该全局规则
- groupBy：以指定字段分组，可指定多个。目前可添加，但查询 Wrapper 中暂不支持构建聚合查询，所以会报错
- parent：当前修饰符构建器所属的父 wrapper（`InfluxQueryWrapper`）。

包含以下方法：

- limit/offset：设置查询数量/偏移量，limit 存在方法重载可同时指定两个参数
- orderBy：按照指定的列，按照指定的排序规则构建排序
- groupBy：按照指定的列构建分组
- build：执行构建，调用父类构建方法，返回完整的结果
- toSql：执行构建，仅构建当前修饰符构建器的部分
- buildGroupBy/buildOrderBy/buildLimit：私有方法，构建分组/排序/限制数量 的修饰符

### InfluxObjMapper

对象转换器，用于将结果集转换为指定的对象集。

提供一系列转换方法，如下：

- map：将一个对象数组转换为指定的类实例。(一个对象数组便是结果集中的一行)
- mapAll：将一个对象数组流转为指定的类实例列表（一个对象数组流便是一个完整的结果集）

- compressToMapList：将一个对象数组流压缩为一个映射表的集合，即结果集中的一行对应着一个映射表
- mapToResult：将一个对象数组转换为一个 InfluxResult 对象，该对象仅包含一行记录
- mapAllToResult：将一个对象数组流转换为一个 InfluxResult 对象，该对象的记录数即为流的元素数
- handleType：静态方法，将一个值根据一个字段所指定的类型处理器进行转换，并返回转换后的值
- getField：静态方法，递归查找指定类中的指定名称字段，用于反射注入

### InfluxQueryWrapper

构建 Influx 查询 SQL 的工具类，允许通过链式调用传递查询参数，并调用方法构建查询语句、执行查询等。

该工具包含前置方法和终结方法，前置方法用于配置查询目标及条件，支持链式调用；终结方法会根据已有状态构造最终查询语句，并返回语句。

#### 构造方法

包含以下构造方法：

- 创建空查询链
- 从映射对象中提取信息
- 从映射对象中提取信息，同时直接构建

#### 添加查询目标列

查询目标列添加方法返回当前工具类实例本身，以支持链式调用。

构建查询参数提供以下方法：

- 传递映射对象添加
- 传递键添加（包括标签、字段）
- 根据当前引用对象添加

同时，在构建查询参数时，需要进行以下操作：

- 检查查询参数是否在映射对象中存在
- 确保查询参数顺序

#### 执行构建（终结方法）

执行构建方法会基于查询参数集合，构建查询 SQL 并返回。

### PointBuilder 端点构造器

由于 InfluxDBClient 在插入数据时，使用的是 `Point 端点`封装类，而 InfluxClient（本项目）使用 `AbstractActionInfluxObj`
进一步封装了操作数据，所以需要此 端点构造器 将数据转换为端点。

包含以下方法：

- build：将一个 AbstractActionInfluxObj 封装为 Point
- buildAll：将一个 AbstractActionInfluxObj 的集合封装为 Point 集合
- valid：验证对象是否合法（是否为空、标签/字段是否为全空、标签/字段 是否有交叉）

# 常见问题

## com.google.protobuf.RuntimeVersion

依赖版本冲突，InfluxDB Client V3 依赖 `org.apache.arrow.flight`，该项目使用 `Protobuf` 的 `4.30.2（或其他）`
版本。而当前项目运行时加载的项目版本更低。Protobuf 要求运行时版本不能低于代码生成的版本。

修改 `pom.xml` 文件，添加以下依赖

```xml
// ... existing code ...
<dependency>
    <groupId>com.wolfhouse</groupId>
    <artifactId>influxclient</artifactId>
    <version>1.1-ALPHA</version>
</dependency>

        <!-- 要填加以下两个依赖 - 强制升级 Protobuf 版本以解决 gencode 版本的冲突 -->
<dependency>
<groupId>com.google.protobuf</groupId>
<artifactId>protobuf-java</artifactId>
<version>4.30.2</version>
</dependency>
<dependency>
<groupId>com.google.protobuf</groupId>
<artifactId>protobuf-java-util</artifactId>
<version>4.30.2</version>
</dependency>
        // ... existing code ...
```

## API response error: write buffer error: parsing for line protocol failed

- 先检查构建的 line protocol 格式是否正确

该问题的原因之一（本项目遇到的）是，在首次插入数据时，InfluxDB 会自动进行类型转换。如对于以下 line protocol，`groud_current` 在
Java 程序中设置为 0，则 InfluxDB 客户端对其转换后的类型是整型。

```
t_detect_record,level-1=1831217700266381314,nodeId=1831217700266381314,sensorTypeId=6 dataStatus=1i,ground_current=0i,plug_current=0i 1765334351201945000
```

但是对于数值型数据，InfluxDB 无法进行类型转换，也就是以下 line protocol 会执行失败。

```
t_detect_record,level-1=1831217700266381314,nodeId=1831217700266381314,sensorTypeId=6 dataStatus=1i,ground_current=0.01,plug_current=0.1 1765334299898996171
```