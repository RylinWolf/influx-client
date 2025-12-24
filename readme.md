# Influx-Client 

对 `InfluxDBClient` 进行二次封装，提供简单的 Query、Insert 操作。

**注意：InfluxDB 的 Java 客户端仅支持 添加、查询数据操作，不支持管理数据。**

# 思路

**添加数据**

客户端通过写入点 `Point` 来创建 line protocol。添加数据时，需要设置写入点的表、标签、字段、时间戳。

将添加的数据封装为抽象对象，包含表、标签、字段、时间戳信息，除时间戳外均需重写。时间戳可选重写，默认为创建对象时的时间。

由于表中每条数据可以有不同的结构，所以表和对象并不是一一对应的。也就是说，一个表可以有多种对象。但是为了方便操作，一个对象只绑定给一个表。

需要注意，标签、字段在查询结果中无法标识，所以类中应当定义该类的标签、字段组。

InfluxDB 插入数据无需关注数据表是否存在，仅需直接插入数据即可。但是表不应随意自定，应当提供简单的约束（比如通过枚举）。

---

**查询数据**

客户端没有提供封装，需要通过 SQL 或 InfluxQL 查询。本客户端选择使用 SQL 作为查询语法。

封装查询 Wrapper，可以通过泛型类指定目标对象，从而通过方法调用动态构建 SQL；也可不指定泛型类，构建指定表、但不受字段约束的 SQL。

查询方法会返回结果流 `Stream<Object[]>`，Object 数组为按照顺序查询的字段的值。可以在构造查询语句时保存查询参数，再根据参数和顺序一一对应到对象中返回。

由于表和对象不是一一对应，所以查询方法应当指定一个泛型，用于封装为指定的对象。

---

**查询条件**

对于查询条件，封装条件 Wrapper，作为查询 Wrapper 的内部类，用于构建通用的查询条件，由查询 Wrapper 尽可能确保条件字段合法性。

通过链式调用添加查询条件。

查询条件分为两大类：

1. 连接条件：and (), or ()，连接其他查询条件语句
2. 基本条件：>、>=、<、<=、= 等，由字段名、值组成

---

**查询约束**

查询约束为查询条件之后的内容，包括限制数量、分组、聚合等。

目前仅计划实现限制数量 (`limit`) 。

---

**结果映射**

`InfluxDbClient` 查询后会返回 `Stream<Object[]>` 流对象，由 Object 数组组成。

每个 Object 数组都是一行数据，一一对应查询参数。



# 实现

## 线程安全 暂未实现

要确保字段、标签类的操作是线程安全的。

目前的想法是自定义线程安全、维护存储顺序的类，通过**ConcurrentHashMap + 读写锁维护顺序**。

## 添加数据

通过 AbstractInsertObj 约束并提供获取标签、字段的键值对方法。需要作为 Influx 对象映射的类应继承该抽象类，并重写其中的 `tableName` 方法，在该方法中返回表名。

需要注意，该抽象类的子类若需要重写抽象类，要注意字段隐藏的问题，修改字段时尽量使用抽象类提供的方法修改，或通过 `super.obj=newObj` 的方式直接修改父类属性。

 

## 查询数据

目前不支持通过查询链对连表、分组、聚合等复杂查询进行简化。

通过 InfluxQueryWrapper 的静态方法创建查询链：

- create：创建匿名查询链，不会约束查询列名
- from：创建指定映射对象的查询链，查询的列名会收到该对象的约束
- fromBuild：创建指定映射对象的查询链并立刻构建查询语句，返回构建后的 SQL 语句

对于查询链，可以链式调用查询方法，从而设置查询目标列。

- select：包含多个重载，可以实现指定字符串字段，或从 InfluxFields、InfluxTags 中导入查询字段
- selectSelfTag：提取已导入的映射对象的标签，作为查询目标
- selectSelfField：提取已导入的映射对象的字段，作为查询目标
- withTime：指定查询目标列中是否包含时间戳，默认为 true

## 结果映射

目前支持查询结果的蛇行命名（下划线命名）和小驼峰命名映射到类对象字段（规定类对象字段应为小驼峰命名法）。

InfluxDB 返回的查询结果集为 Object 数组流，每个数组对应着一行数据。

通过查询链的属性，可以获取到维护顺序的查询字段，根据结果集下标与查询字段顺序，对字段和结果进行一一映射，构建为最终的结果集。

最终的结果集可以有多种形态：

- Map：字符串到 Object 的映射，最基本的结果集形态
- AbstractBeanInfluxObj 的子类：自定义的类，继承该父类后添加字段，工具将通过反射构建实例，并通过结果集的字段名为实例字段注入数据，最终返回该类的实例
- InfluxResult：本工具提供的 AbstractBeanInfluxObj 的子类，维护一个内部类 InfluxRow 的列表，通过该内部类映射数据行（一个数据行对应一个结果集），核心基于 Map 实现。



# 使用方法

## 添加数据



## 查询数据

### 构建查询目标

### 构建查询条件



# 主要相关类

## 基础类

### InfluxFields

 

### InfluxTags



### AbstractBaseInfluxObj

### AbstractActionInfluxObj

### InfluxResult

### InfluxPage



### 



## 核心类

### BaseSqlBuilder

### InfluxConditionWrapper

### InfluxModifiersWrapper

### InfluxObjMapper

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

### PointBuilder



## 处理器类

### TypeHandler

### InfluxTypeHandler

### InstantTypeHandler

### 

## 工具类

### TimeStampUtils



# 常见问题

## com.google.protobuf.RuntimeVersion

依赖版本冲突，InfluxDB Client V3 依赖 `org.apache.arrow.flight`，该项目使用 `Protobuf` 的 `4.30.2（或其他）` 版本。而当前项目运行时加载的项目版本更低。Protobuf 要求运行时版本不能低于代码生成的版本。

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

  

该问题的原因之一（本项目遇到的）是，在首次插入数据时，InfluxDB 会自动进行类型转换。如对于以下 line protocol，`groud_current` 在 Java 程序中设置为 0，则 InfluxDB 客户端对其转换后的类型是整型。

```
t_detect_record,level-1=1831217700266381314,nodeId=1831217700266381314,sensorTypeId=6 dataStatus=1i,ground_current=0i,plug_current=0i 1765334351201945000
```

但是对于数值型数据，InfluxDB 无法进行类型转换，也就是以下 line protocol 会执行失败。

```
t_detect_record,level-1=1831217700266381314,nodeId=1831217700266381314,sensorTypeId=6 dataStatus=1i,ground_current=0.01,plug_current=0.1 1765334299898996171
```



# 当前问题

- measurement 隶属于对象，而不是类。应当通过注解方式将表与类绑定
- 由于以上条，InfluxQueryWrapper 现在引用的是对象，而不是指定的类。解决以上问题后，要重构 InfluxQueryWrapper。
- InfluxQueryWrapper 中，引用的映射对象，无法区分约束字段和要查询的字段。
- 每次查询时，都要创建一个新的对象。可以提高兼容度，对于不需要区分标签和字段的类，通过注解控制是否反射获取类字段作为查询参数（需要进一步思考，现在要对硬编码的传感器类型解耦）