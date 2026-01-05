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

本项目的主要类可以分为以下类别：

- 通用类：包括配置类、工具类、处理器类等；
- 映射对象相关类：InfluxDB 数据表中的每一条记录，都可以映射到一个 Java 对象中，又称为 **领域对象（Domian）**。由于 InfluxDB 表结构的特殊性（不预先设定架构，且列名分为标签、字段两种），对于每个表，单独的一个领域对象无法很好表示。以上，为很好地映射一个 InfluxDB 记录，需要通过一系列类；
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

- paramters：查询条件参数占位名与对应值的映射，键为自动构建的查询参数占位名，值为其应被替换的值。如：param_1 -> 1，则条件 SQL 中 ${param_1} 最终会被替换为 1。通过此解决 SQL 注入的问题。
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

由于 InfluxDBClient 在插入数据时，使用的是 `Point 端点`封装类，而 InfluxClient（本项目）使用 `AbstractActionInfluxObj` 进一步封装了操作数据，所以需要此 端点构造器 将数据转换为端点。

包含以下方法：

- build：将一个 AbstractActionInfluxObj 封装为 Point
- buildAll：将一个 AbstractActionInfluxObj 的集合封装为 Point 集合
- valid：验证对象是否合法（是否为空、标签/字段是否为全空、标签/字段 是否有交叉）

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