# IGinX 安装使用教程（一键启动）

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

### IGinX 安装

IGinX 为系统的主体部分，通过一键启动安装包

```shell
$ cd ~
$ wget https://github.com/IGinX-THU/IGinX/releases/download/v0.7.0/IGinX-FastDeploy-0.7.0.tar.gz
$ tar -xzvf IGinX-FastDeploy-v0.7.0-bin.tar.gz
```

## 启动

```shell
$ cd ~
$ cd IGinX-FastDeploy-0.7.0
$ chmod +x ./runIginxOn1Host.sh
$ ./runIginxOn1Host.sh
```

显示出如下字样，表示 IGinX 启动成功：

```shell
ZooKeeper is started!
IoTDB is started!
IGinX is started!
=====================================
You can now test IGinX. Have fun!~
=====================================
```

若在Windows系统环境下出现找不到文件路径问题，则需要使用文本编辑器修改根目录下的`runIginxOn1Host.bat`

```shell
#./runIginxOn1Host.bat
# line 2
start "zookeeper" /d "include/apache-zookeeper-3.7.0-bin/" bin\zkServer.cmd
# ↓ 改为
start "zookeeper" /d "include\apache-zookeeper-3.7.0-bin\" bin\zkServer.cmd

# line 5
start "iotdb" /d "include/apache-iotdb-0.12.6/sbin" start-server.bat
# ↓ 改为
start "iotdb" /d "include\apache-iotdb-0.12.6\sbin" start-server.bat

# line 9
start "iginx" /d "./sbin" start_iginx.bat
# ↓ 改为
start "iginx" /d ".\sbin" start_iginx.bat
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
$ wget https://github.com/IGinX-THU/IGinX/archive/refs/tags/v0.7.0.zip
# 解压源码包
$ tar -zxvf v0.7.0.tar.gz
# 进入项目主目录
$ cd IGinX-release-v0.7.0
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

完整版使用代码可以参考：https://github.com/IGinX-THU/IGinX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/IoTDBSessionExample.java
