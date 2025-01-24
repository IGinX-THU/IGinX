# IGinX 安装使用教程（编译安装）

[TOC]

IGinX是清华大学“清华数为”大数据软件栈的“大数据总线”，面向解决用户在大数据场景下“管数烦、用数难”的问题而研发。它的特色包括“负载均衡弹性好、异构关联全局化、数据使用不搬家、Python集成便利大、SQL输入实时查”。

IGinX支持用户一体化管理已存储在不同系统中的数据资产，也支持用户统一读写、查询、关联特定系统中的数据。目前，IGinX支持一体化管理包括关系数据库PostgreSQL、时序数据库InfluxDB/IotDB/TimescaleDB/OpenTSDB、大数据文件Parquet集合等存储的数据。

## 安装

### Java 安装

由于 ZooKeeper、IGinX 以及 IoTDB 都是使用 Java 开发的，因此首先需要安装 Java。如果本地已经安装了 JDK>=1.8 的运行环境，**直接跳过此步骤**。

1. 首先访问 [Java官方网站](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html)下载适用于当前系统的 JDK 包。
2. 安装

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # 解压文件
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

3. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

4. 使用 java -version 判断 JDK 是否安装成功

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

如果显示出如上的字样，则表示安装成功。

### Maven 安装

Maven 是 Java 项目管理和自动构建工具，如果您需要从源码进行编译，还需要安装 Maven >= 3.6 的环境，否则，**直接跳过此步骤**。

1. 访问[官网](http://maven.apache.org/download.cgi)下载并解压 Maven

```
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz

$ tar -xvf  apache-maven-3.3.9-bin.tar.gz

$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

2. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下的两行：

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

3. 使用 mvn -v 判断 Maven 是否安装成功

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

如果显示出如上的字样，则表示安装成功。

### Node.js 安装

Node.js是跨平台、轻量级的JavaScript运行环境，项目使用npm管理相关的Node.js包。

1. 访问[官网](https://nodejs.org/en/download)，下载并解压Node.js

```shell
$ cd ~
$ wget https://nodejs.org/dist/v18.16.1/node-v18.16.1-linux-x64.tar.xz
$ tar -zxvf node-v18.16.1-linux-x64.tar.xz
$ sudo mv -f node-v18.16.1-linux-x64 /usr/local/
```

2. 设置路径

编辑 ~/.bashrc 文件，在文件末端加入如下两行：

```shell
export NODEJS_HOME=/usr/local/node-v18.16.1-linux-x64
export PATH=${PATH}:${NODEJS_HOME}/bin
```

加载更改后的配置文件：

```shell
$ source ~/.bashrc
```

3. 使用 npm -v 判断是否安装成功

```shell
$ npm -v
9.5.1
```

### ZooKeeper 安装

ZooKeeper 是 Apache 推出的开源的分布式应用程序协调服务。如果您需要部署大于一个 IGinX 实例，则需要安装 ZooKeeper

具体安装方式如下：

1. 访问[官网](https://zookeeper.apache.org/releases.html)下载并解压 ZooKeeper

```shell
$ cd ~
$ wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz
$ tar -zxvf apache-zookeeper-3.7.2-bin.tar.gz
```

2. 修改 ZooKeeper 默认配置文件

```shell
$ cd apache-zookeeper-3.7.2-bin/
$ mkdir data
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

然后编辑 conf/zoo.cfg 文件，将

```shell
dataDir=/tmp/zookeeper
```

修改为

```shell
dataDir=data
```

### IGinX 安装

拉取最新开发版本，并进行本地构建

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git
$ cd IGinX
$ mvn clean install -Dmaven.test.skip=true
```

显示出如下字样，表示 IGinX 构建成功：

```shell
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary for IGinX 0.8.0-SNAPSHOT:
[INFO]
[INFO] IGinX .............................................. SUCCESS [ 20.674 s]
[INFO] IGinX Thrift ....................................... SUCCESS [01:18 min]
[INFO] IGinX Shared ....................................... SUCCESS [  8.101 s]
[INFO] IGinX Session ...................................... SUCCESS [  3.168 s]
[INFO] IGinX Antlr ........................................ SUCCESS [ 16.170 s]
[INFO] IGinX Core ......................................... SUCCESS [03:35 min]
[INFO] IGinX Client ....................................... SUCCESS [ 11.159 s]
[INFO] IGinX JDBC ......................................... SUCCESS [  3.426 s]
[INFO] IGinX IoTDB12 ...................................... SUCCESS [ 31.081 s]
[INFO] IGinX InfluxDB ..................................... SUCCESS [ 17.516 s]
[INFO] IGinX OpenTSDB ..................................... SUCCESS [ 11.103 s]
[INFO] IGinX PostgreSQL ................................... SUCCESS [  4.449 s]
[INFO] IGinX Parquet ...................................... SUCCESS [01:28 min]
[INFO] IGinX Redis ........................................ SUCCESS [  9.875 s]
[INFO] IGinX MongoDB ...................................... SUCCESS [ 10.020 s]
[INFO] IGinX Example ...................................... SUCCESS [  3.864 s]
[INFO] IGinX Test ......................................... SUCCESS [  5.490 s]
[INFO] Zeppelin on IGinX .................................. SUCCESS [ 25.019 s]
[INFO] IGinX Tools for CSV export ......................... SUCCESS [  2.860 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  09:25 min
[INFO] Finished at: 2023-06-20T10:28:24+08:00
[INFO] ------------------------------------------------------------------------
```

## 启动

### ZooKeeper

启动IGinX前，需要启动ZooKeeper。

```shell
$ cd ~
$ cd apache-zookeeper-3.7.2-bin/
$ ./bin/zkServer.sh start
```

显示出如下字样，表示 ZooKeeper 启动成功

```shell
ZooKeeper JMX enabled by default
Using config: /home/root/apache-zookeeper-3.7.2-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

### IGinX

修改`IGINX_HOME/conf/config.properties`，设置时序数据库为parquet

```properties
storageEngineList=127.0.0.1#6667#parquet#dir=parquetData#has_data=false#is_read_only=false
# dir=parquetData将在工作目录创建一个parquetData文件夹，用户也可以修改为其他路径（工作目录中）
```

使用源码启动

```shell
$ cd ~
$ cd IGinX/core/target/iginx-core-0.8.0-SNAPSHOT
$ chmod +x sbin/start_iginx.sh # 为启动脚本添加启动权限
$ ./sbin/start_iginx.sh
```

显示出如下字样，表示 IGinX 启动成功：

```shell
2023-06-30 10:32:05,260 [Thread-3]  INFO - [cn.edu.tsinghua.iginx.parquet.server.ParquetServer.startServer:42] parquet service starts successfully!


IGinX is now in service......
```

## 访问 IGinX

### RESTful 接口

启动完成后，可以便捷地使用 RESTful 接口向 IGinX 中写入并查询数据。

创建文件 insert.json，并向其中添加如下的内容：

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2 ],
        [1359788410000, 23.1 ]
    ],
    "tags": {
        "host": "server1",
        "data_center": "DC1"
    }
  },
  {
      "name": "archive_file_search",
      "timestamp": 1359786400000,
      "value": 321,
      "tags": {
          "host": "server2"
      }
  }
]
```

使用如下的命令即可向数据库中插入数据：

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:7888/api/v1/datapoints
```

在插入数据后，还可以使用 RESTful 接口查询刚刚写入的数据。

创建文件 query.json，并向其中写入如下的数据：

```json
{
	"start_absolute" : 1,
	"end_relative": {
		"value": "5",
		"unit": "days"
	},
	"time_zone": "Asia/Kabul",
	"metrics": [
		{
		"name": "archive_file_tracked"
		},
		{
		"name": "archive_file_search"
		}
	]
}
```

使用如下的命令查询数据：

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:7888/api/v1/datapoints/query
```

命令会返回刚刚插入的数据点信息：

```json
{
    "queries": [
        {
            "sample_size": 3,
            "results": [
                {
                    "name": "archive_file_tracked",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "data_center": [
                            "DC1"
                        ],
                        "host": [
                            "server1"
                        ]
                    },
                    "values": [
                        [
                            1359788300000,
                            13.2
                        ],
                        [
                            1359788400000,
                            123.3
                        ],
                        [
                            1359788410000,
                            23.1
                        ]
                    ]
                }
            ]
        },
        {
            "sample_size": 1,
            "results": [
                {
                    "name": "archive_file_search",
                    "group_by": [
                        {
                            "name": "type",
                            "type": "number"
                        }
                    ],
                    "tags": {
                        "host": [
                            "server2"
                        ]
                    },
                    "values": [
                        [
                            1359786400000,
                            321.0
                        ]
                    ]
                }
            ]
        }
    ]
}
```

更多接口可以参考 [IGinX 官方手册](../pdf/userManualC.pdf) 。

### RPC 接口

除了 RESTful 接口外，IGinX 还提供了 RPC
的数据访问接口，具体接口参考 [IGinX 官方手册](../pdf/userManualC.pdf)，同时 IGinX
还提供了部分[官方 example](https://github.com/IGinX-THU/IGinX/tree/main/example/src/main/java/cn/edu/tsinghua/iginx/session)，展示了
RPC 接口最常见的用法。

下面是一个简短的使用教程。

由于目前 IGinX jar包还未发布到 maven 中央仓库，因此如需使用的话，需要手动安装到本地的 maven 仓库。具体安装方式如下：

```shell
# 下载 IGinX 最新release 版本源码包
$ wget https://github.com/IGinX-THU/IGinX/archive/refs/tags/v0.7.0.tar.gz
# 解压源码包
$ tar -zxvf v0.7.0.tar.gz
# 进入项目主目录
$ cd IGinX-rc-v0.7.0
# 安装到本地 maven 仓库
$ mvn clean install -DskipTests
```

具体在使用时，只需要在相应的项目的 pom 文件中引入如下的依赖：

```xml
<dependency>
  	<groupId>cn.edu.tsinghua</groupId>
  	<artifactId>iginx-core</artifactId>
  	<version>0.7.0</version>
</dependency>
```

在访问 IGinX 之前，首先需要创建 session，并尝试连接。Session 构造器有 4 个参数，分别是要连接的 IGinX 的 ip，port，以及用于 IGinX 认证的用户名和密码。目前的权鉴系统还在编写中，因此访问后端
IGinX 的账户名和密码直接填写 root 即可：

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

随后可以尝试向 IGinX 中插入数据。由于 IGinX 支持在数据首次写入时创建时间序列，因此并不需要提前调用相关的序列创建接口。IGinX 提供了行式和列式的数据写入接口，以下是列式数据写入接口的使用样例：

```java
private static void insertColumnRecords(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        int size = 1500;
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = i;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[size];
            for (long j = 0; j < size; j++) {
                if (i < 2) {
                  values[(int) j] = i + j;
                } else {
                  values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
                }
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 0; i < 2; i++) {
            dataTypeList.add(DataType.BINARY);
        }

        session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
}
```

在完成数据写入后，可以使用数据查询接口查询刚刚写入的数据：

```java
private static void queryData(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");
        paths.add("sg.d3.s3");
        paths.add("sg.d4.s4");

        long startTime = 100L;
        long endTime = 200L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        dataSet.print();
}
```

还可以使用降采样聚合查询接口来查询数据的区间统计值：

```java
private static void downsampleQuery(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");

        long startTime = 100L;
        long endTime = 1101L;

        // MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        // FIRST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, ROW_INTERVAL * 100);
        dataSet.print();

        // LAST
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, ROW_INTERVAL * 100);
        dataSet.print();

        // COUNT
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, ROW_INTERVAL * 100);
        dataSet.print();

        // SUM
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, ROW_INTERVAL * 100);
        dataSet.print();

        // AVG
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, ROW_INTERVAL * 100);
        dataSet.print();

}

```

最终使用完 session 后需要手动关闭，释放连接：

```shell
session.closeSession();
```

完整版使用代码可以参考：https://github.com/IGinX-THU/IGinX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/ParquetSessionExample.java

## 常见问题

### 找不到 parquet-file 依赖

在使用 Maven 编译 IGinX 时，可能会遇到如下的错误：

```shell
The following artifacts could not be resolved: cn.edu.tsinghua.iginx:parquet-file
```

这是因为 IGinX 依赖的 parquet-file 依赖并未发布到 Maven 中央仓库，而是托管在了 GitHub Pages 中。因此，我们在 pom.xml 文件中添加如下的仓库地址：

```xml
<repositories>
    <repository>
        <id>parquet-file</id>
        <name>IGinX GitHub repository</name>
        <url>https://iginx-thu.github.io/Parquet/maven-repo</url>
    </repository>
</repositories>
```

如果你配置了镜像源，例如：

```xml
<mirror>
    <id>aliyunmaven</id>
    <mirrorof>central</mirrorOf>
    <name>阿里云公共仓库</name>
    <url>https://maven.aliyun.com/repository/central</url>
</mirror>
```

注意，这里的 mirrorOf 的值为 central，表示只有在访问 Maven 中央仓库时才会使用阿里云的镜像源。
如果你配置为 *，则表示所有的 Maven 仓库都会使用阿里云的镜像源，这会造成 IGinX 依赖的 parquet-file 依赖无法下载。

如果由于网络原因无法下载依赖，可以尝试使用代理的方式解决。 此外，还可以手动下载依赖，然后使用 Maven 的 install 命令将其安装到本地仓库，
详见：https://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html
