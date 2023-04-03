# IGinX测试代码添加规范

IGinX代码测试框架于2023年完成重构，测试主要分为以下部分

### 测试说明

- **`DB-CE`**：数据库扩容测试
  - 文件位于`integration/expansion/CapacityExpansionIT.java`
  - 对每种数据库，进行四种测试场景，`OriNoDataExpNoData`（原节点无数据扩容节点无数据）、`OriNoDataExpHasData`（原节点无数据扩容节点有数据）、`OriHasDataExpNoData`（原节点有数据扩容节点无数据）、`OriHasDataExpHasData`（原节点有数据扩容节点有数据）。每个测试函数测试原始数据库是否有数据和扩展数据库是否有数据的不同组合。在每种情况下，还将对现有功能测试（如TagIT，RestIT等）进行检验
  - 测试文件还包含一个名为`testPrefixAndRemoveHistoryDataSource`的函数，用于测试前缀功能。该函数添加两个具有不同前缀的存储引擎，从每个引擎中查询数据，删除其中一个存储引擎，然后再次查询数据，以确保不再可以访问被删除的存储引擎
  - 测试文件使用SQLTestTools类执行SQL语句，并将结果与预期值进行比较
- **`standalone-test`**：单机数据库功能测试
  - 文件位于`integration/controller/Controller.java`
  - `testUnion()`方法负责在一组存储引擎上运行测试。首先加载测试配置文件，并提取要测试的存储引擎的相关特性。并将其支持的测试文件写入待测试列表中，然后调用脚本对数据库一一执行测试列表中的测试。
  - 目前测试的包括`SQLSessionIT,SQLSessionPoolIT,TagIT,RestAnnotationIT,RestIT,TransformIT,UDFIT,SessionV2IT,BaseSessionIT,BaseSessionPoolIT`
- **`unit-mds`**：元数据功能测试
  - 文件位于`integration/mds/`下
  - 分别针对ETCD和ZOOKEEPER进行测试
- **`unit-test`**：单元测试
  - 文件位于`src/core/`下
  - 针对core中的所有的UT进行测试
- **`case-regression`**：混合测试
  - 文件位于`integration/regression/`下
  - 混合数据库测试

### 添加测试以及被测数据库

目前我们通常会添加测试方法和被测数据库对象，这里将讲解如何快速添加

##### 添加被测数据库

- **`添加数据库单机测试`**
  1、在`testConfig.properties`中添加对应的数据库信息。在其中的`storageEngineList`中添加数据库名称；添加数据库连接方式；添加要测试的IT名称，如果不添加则默认测试全套IT测试，即test-list；添加数据库支持的属性

  2、在`standalone-test`中的矩阵部分`DB-name`添加对应数据库名称，注意大小写敏感

- **`添加数据库扩容测试`**
  1、首先在`DB-CE.yml`中的环境变量`env`声明中添加对应数据库测试列表，如`IoTDB12FUNCTEST`。然后在矩阵部分`DB-name`添加对应数据库名称，注意大小写敏感，即这里的数据库名称要和上述`env`中的数据名称完全一样

  2、在`integration/expansion`下添加实现数据库的历史数据写入方法。仿照其他数据库继承`BaseHistoryDataGenerator`类实现扩容测试，需要覆写`addStorageWithPrefix`和`getPort`两个方法。实现`BaseHistoryDataGenerator`接口，仿照其他数据库，实现一个写入数据逻辑，其中需要写入的数据已经通过参数传入。

***Few bugs, highest quality. Precise and polished. Have a good day!***
