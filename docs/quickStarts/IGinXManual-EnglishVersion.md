# IGinX Installation and Use Manual

[TOC]

IGinX is an open source polystore system. A polystore system provides an integrated data management service over a set of one or more potentially heterogeneous database/storage engines, serving heterogeneous workloads.

Currently, IGinX directly supports big data service over relational database PostgreSQL, time series databases InfluxDB/IoTDB/TimescaleDB/OpenTSDB, and Parquet data files.

## Download and Installation

IGinX provides you with two installation methods.
You can refer to the following suggestions and choose either of them:

1. Download the installation package from the official website. This is our recommended installation method, through which you will get a packaged executable binary file that can be used immediately.
2. Compile with source code. If you need to modify code yourself, you can use this installation method.

### Environmental Preparation

#### Java Installation

Since ZooKeeper, IGinX and IoTDB are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

1. First, visit the [official Java website](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download the JDK package for your current system.
2. Installation

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # unzip files
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

1. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

2. Use java -version to determine whether JDK installed successfully.

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

If the words above are displayed, it means the installation was successful.

#### Maven Installation

Maven is a build automation tool used primarily to build and managa Java projects. If you need to compile from the source code, you also need to install a Maven environment >= 3.6. Otherwise, **skip this step entirely**.

This step has  been added- before installing ZooKeeper, you may check that you have the installation of wget. It will save time.

1. Visit the [official website](http://maven.apache.org/download.cgi)to download and unzip Maven

```shell
$ wget http://mirrors.hust.edu.cn/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
$ tar -xvf  apache-maven-3.3.9-bin.tar.gz
$ sudo mv -f apache-maven-3.3.9 /usr/local/
```

2. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export MAVEN_HOME=/usr/local/apache-maven-3.3.9
export PATH=${PATH}:${MAVEN_HOME}/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

3. Type mvn -v to determine whether Maven installed successfully.

```shell
$ mvn -v
Apache Maven 3.6.1 (d66c9c0b3152b2e69ee9bac180bb8fcc8e6af555; 2019-04-05T03:00:29+08:00)
```

If the words above are displayed, that means the installation was successful.

#### ZooKeeper installation

ZooKeeper is an open-source server for highly reliable distributed coordination of cloud applications, launched by Apache. If you need to deploy more than one instance of IGinX, you will need to install ZooKeeper. Otherwise, **skip this step entirely**

The specific installation method is as follows,

1. Visit the [official website](https://zookeeper.apache.org/releases.html)to download and unzip ZooKeeper

```shell
$ cd ~
$ wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz
$ tar -zxvf apache-zookeeper-3.7.2-bin.tar.gz
```

2. Modify the default ZooKeeper profile

```shell
$ cd apache-zookeeper-3.7.2-bin/
$ mkdir data
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

Then edit the conf/zoo.cfg file and

```shell
dataDir=/tmp/zookeeper
```

Modify to

```shell
dataDir=data
```

#### IoTDB Installation

IoTDB is Apache's Apache IoT native database with high performance for data management and analysis, deployable on the edge and the cloud.

The specific installation method is as follows:

```shell
$ cd ~
$ wget https://github.com/IGinX-THU/IGinX-resources/raw/main/resources/apache-iotdb-0.12.6-server-bin.zip
$ unzip apache-iotdb-0.12.6-server-bin.zip
```

### Download the binary executables

Go directly to the [IGinX project](https://github.com/IGinX-THU/IGinX) and download the [IGinX project release package](https://github.com/IGinX-THU/IGinX/releases/download/v0.7.0/IGinX-Server-0.7.0.tar.gz).

```shell
$ cd ~
$ wget https://github.com/IGinX-THU/IGinX/releases/download/v0.7.0/IGinX-Server-0.7.0.tar.gz
$ tar -xzvf IGinX-Server-0.7.0.tar.gz
```

### Compilation with source code

Fetch the latest development version and build it locally.

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git
$ cd IGinX
$ mvn clean install -Dmaven.test.skip=true
```

## Configure launch

### Single node

Single node configuration refers to a single instance of IGinX, and the backend serves as the launch of a single IoTDB instance.

#### Launch IoTDB

First of all, you need to launch IoTDB.

```shell
$ cd ~
$ cd apache-iotdb-0.12.6-server-bin/
$ ./sbin/start-server.sh
```

The following display of words means the IoTDB installation was successful：

```shell
2021-05-27 08:21:07,440 [main] INFO  o.a.i.d.s.t.ThriftService:125 - IoTDB: start RPC ServerService successfully, listening on ip 0.0.0.0 port 6667
2021-05-27 08:21:07,440 [main] INFO  o.a.i.db.service.IoTDB:129 - IoTDB is set up, now may some sgs are not ready, please wait several seconds...
2021-05-27 08:21:07,448 [main] INFO  o.a.i.d.s.UpgradeSevice:109 - finish counting upgrading files, total num:0
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:74 - Waiting for upgrade task pool to shut down
2021-05-27 08:21:07,449 [main] INFO  o.a.i.d.s.UpgradeSevice:76 - Upgrade service stopped
2021-05-27 08:21:07,449 [main] INFO  o.a.i.db.service.IoTDB:146 - Congratulation, IoTDB is set up successfully. Now, enjoy yourself!
2021-05-27 08:21:07,450 [main] INFO  o.a.i.db.service.IoTDB:93 - IoTDB has started.
```

#### Launch ZooKeeper

If you are taking a binary installation package, or if you designate Zookeeper as the metadata management storage backend in the configuration file, you need to launch ZooKeeper. Otherwise, **skip this step entirely**.

```shell
$ cd ~
$ cd apache-zookeeper-3.7.2-bin/
$ ./bin/zkServer.sh start
```

The following display of words means the ZooKeeper installation was successful：

```shell
ZooKeeper JMX enabled by default
Using config: /home/root/apache-zookeeper-3.7.2-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

#### Launch IGinX

Using the release package to launch

```shell
$ cd ~
$ cd IGinX-Server-0.7.0
$ chmod +x startIginX.sh # enable permissions for startup scripts
$ ./startIginX.sh
```

Using source code to launch

```shell
$ cd ~
$ cd Iginx
$ chmod +x startIginX.sh # enable permissions for startup scripts
$ ./startIginX.sh
```

### Cluster

IGinX now has two method options: to use ZooKeeper storage or write local files.

When the deployment scenario is multiple IGinX instances, you must use ZooKeeper storage, deploy a single IGinX instance and use the source code compilation and installation method. Both can be selected, just change the corresponding configuration file.

Take two IoTDB instances and two IGinX instances as examples.

#### Launching multiple IoTDB instances

Here is an example of starting two instances with ports 6667 and 7667, respectively, on a single machine.

```shell
$ cd ~
$ cd apache-iotdb-0.12.6-server-bin/
$ ./sbin/start-server.sh # 启动实例一 127.0.0.1: 6667
```

Modify the configuration file IoTDB_HOME/conf/iotdb-engine.properties

```shell
rpc_port=7667
```

Start the second instance.

```shell
$ ./sbin/start-server.sh # 启动实例二 127.0.0.1: 7667
```

#### Starting multiple IGinX instances

Modify IGinX_HOME/conf/config. Properties

```shell
storageEngineList=127.0.0.1#6667#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/,127.0.0.1#6688#iotdb#username=root#password=root#sessionPoolSize=100#dataDir=/path/to/your/data/

#存储方式选择 ZooKeeper
metaStorage=zookeeper 

# 提供ZooKeeper端口
zookeeperConnectionString=127.0.0.1:2181

#注释掉file、etcd相关配置
#fileDataDir=meta
#etcdEndpoints=http://localhost:2379
```

Start the first IGinX instance.

```shell
$ cd ~
$ cd Iginx
$ chmod +x sbin/start_iginx.sh # 为启动脚本添加启动权限
$ ./sbin/start_iginx.sh
```

Modify conf/config. Properties

```shell
# iginx 绑定的端口
port=7888
# rest 绑定的端口
restPort=7666
```

Launch a second instance of IGinX.

```shell
$ ./sbin/start_iginx.sh
```

### Configuration Items

In order to facilitate installation and management of IGinX, IGinX provides users with several optional item configurations. The IGinX configuration file is located in `config.properties` under the `$IGinX_HOME/conf` folder of the IGinX installation directory. It mainly includes three aspects of configuration: IGinX, Rest, and metadata management.

#### IGinX Configuration

|      Configuration Item      |                                 Description                                 |                                                                                                                                                             Configuration                                                                                                                                                             |
|------------------------------|-----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ip                           | iginx ip bounds                                                             | 0.0.0.0                                                                                                                                                                                                                                                                                                                               |
| port                         | iginx back-end port                                                         | 6888                                                                                                                                                                                                                                                                                                                                  |
| username                     | iginx username                                                              | root                                                                                                                                                                                                                                                                                                                                  |
| password                     | iginx password                                                              | root                                                                                                                                                                                                                                                                                                                                  |
| storageEngineList            | Time series database list, use ',' to separate different instances          | 127.0.0.1#6667#iotdb12#username=root#password=root#sessionPoolSize=20#has_data=false#is_read_only=false                                                                                                                                                                                                                               |
| maxAsyncRetryTimes           | The maximum number of repetitions of asynchronous requests                  | 3                                                                                                                                                                                                                                                                                                                                     |
| asyncExecuteThreadPool       | Asynchronous execution concurrent number                                    | 20                                                                                                                                                                                                                                                                                                                                    |
| syncExecuteThreadPool        | The number of concurrent executions                                         | 60                                                                                                                                                                                                                                                                                                                                    |
| replicaNum                   | number of copies written                                                    | 1                                                                                                                                                                                                                                                                                                                                     |
| databaseClassNames           | The underlying database class name, use ',' to separate different databases | iotdb12=cn.edu.tsinghua.iginx.iotdb.IoTDBStorage,influxdb=cn.edu.tsinghua.iginx.influxdb.InfluxDBStorage,parquet=cn.edu.tsinghua.iginx.parquet.ParquetStorage,relational=cn.edu.tsinghua.iginx.relational.RelationAbstractStorage,mongodb=cn.edu.tsinghua.iginx.mongodb.MongoDBStorage,redis=cn.edu.tsinghua.iginx.redis.RedisStorage |
| policyClassName              | Policy class name                                                           | cn.edu.tsinghua.iginx.policy.naive.NaivePolicy                                                                                                                                                                                                                                                                                        |
| statisticsCollectorClassName | Statistics collection class                                                 | cn.edu.tsinghua.iginx.statistics.StatisticsCollector                                                                                                                                                                                                                                                                                  |
| statisticsLogInterval        | Statistics print interval, in milliseconds                                  | 1000                                                                                                                                                                                                                                                                                                                                  |

When connecting to relational databases such as PostgreSQL and MySQL, you can configure the parameters of the HikariDataSource at `storageEngineList`

|    Configuration Item     |                           Description                           | Defalt |
|---------------------------|-----------------------------------------------------------------|--------|
| connection_timeout        | Connection timeout (ms)                                         | 30000  |
| idle_timeout              | Idle connection timeout (ms)                                    | 10000  |
| maximum_pool_size         | The maximum number of connections in the connection pool        | 20     |
| minimum_idle              | Minimum number of idle connections in the connection pool       | 1      |
| leak_detection_threshold  | Threshold for detecting connection leaks (ms)                   | 2500   |
| prep_stmt_cache_size      | Number of SQL precompiled objects cached                        | 250    |
| prep_stmt_cache_sql_limit | The upper limit of the number of SQL precompiled objects cached | 2048   |

#### Rest Configuration

| Configuration item |          Description           | Default value |
|--------------------|--------------------------------|---------------|
| restIp             | rest-bound ip                  | 0.0.0.0       |
| restPort           | rest bound port                | 7888          |
| enableRestService  | Whether to enable rest service | true          |

#### Metadata Configuration

| Configuration item | Description | Default value |
| ------------------------- | ----------------------- ------------------------------------- | ------------ --------- |
| metaStorage | Metadata storage type, optional zookeeper, file, etcd | file |
| fileDataDir | If you use file as the metadata storage backend, you need to provide | meta |
| zookeeperConnectionString | If you use zookeeper as the metadata storage backend, you need to provide | 127.0.0.1:2181 |
| etcdEndpoints | If using etcd as the metadata storage backend, need to provide, if there are multiple etcd instances, separated by commas | http://localhost:2379 |

## Access

### RESTful Interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IGinX.

Create the file insert.json and add the following to it:

```json
[
  {
    "name": "archive_file_tracked",
    "datapoints": [
        [1359788400000, 123.3],
        [1359788300000, 13.2],
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

Use the following command to insert data into the database:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:7888/api/v1/datapoints
```

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data to it:

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

Use the following command to query the data:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @query.json http://127.0.0.1:7888/api/v1/datapoints/query
```

The command will return information about the data point just inserted:

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

For more interfaces, please refer to [IGinX Official Manual](../pdf/userManualC.pdf).

### RPC Interface

In addition to the RESTful interface, IGinX also provides RPC data access interface. For that specific interface, please refer to the official[IGinX Official Manual](../pdf/userManualC.pdf). At the same time, IGinX also provides some [official examples](https://github.com/IGinX-THU/IGinX/tree/main/example/src/main/java/cn/edu/tsinghua/iginx/session), showing the most common usage of the RPC interface.

Below is a short tutorial on how to use it.

Since the IGinX jars have not been released to the maven central repository, if you want to use it, you need to manually install it to the local maven repository. The specific installation method is as follows:

```shell
# Download the newest IGinX version source package
$ wget https://github.com/IGinX-THU/IGinX/archive/refs/tags/v0.7.0.tar.gz
# Unzip the source package
$ tar -zxvf v0.7.0.tar.gz
# Enter the project main directory
$ cd IGinX-v0.7.0
# Install to local maven repository
$ mvn clean install -DskipTests
```

Only when you are using it, you need to introduce the following dependencies in the pom file of the corresponding project:

```xml
<dependency>
  <groupId>cn.edu.tsinghua</groupId>
  <artifactId>iginx-core</artifactId>
  <version>0.7.0</version>
</dependency>
```

Before accessing IGinX, you first need to open a session and try to connect. The session constructor has 4 parameters, which are the ip and port of IGinX to connect to, and the username and password for IGinX authentication. The current authentication system is still being written, so the account name and password to access the backend IGinX can directly fill in root:

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

You can then try to insert data into IGinX. Since IGinX supports the creation of time series when data is written for the first time, there is no need to call the relevant series creation interface in advance. IGinX provides a row-style and column-style data-writing interface. The following is a usage example of the column-style data writing interface:

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
            Object[] values ​​= new Object[size];
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

After completing the data writing, you can use the data query interface to query the data just written:

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

You can also use the downsampling aggregation query interface to query the interval statistics of the data:

```java
private static void downsampleQuery(Session session) throws SessionException, ExecutionException {
        List<String> paths = new ArrayList<>();
        paths.add("sg.d1.s1");
        paths.add("sg.d2.s2");

        long startTime = 100L;
        long endTime = 1101L;

        //MAX
        SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, 100);
        dataSet.print();

        // MIN
        dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
        dataSet.print();

        //FIRST
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

After the session is completed, you need to manually close and release your connection from your terminal/backend:

```shell
session.closeSession();
```

For the full version of the code, please refer to: https://github.com/IGinX-THU/IGinX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/IoTDBSessionExample.java

## Reference IGinX class library based on MAVEN

### Using POM

        <repositories>
                <repository>
                    <id>github-release-repo</id>
                    <name>The Maven Repository on Github</name>
                    <url>https://github.com/IGinX-THU/IGinX/maven-repo/</url>
                </repository>
        </repositories>
        <dependencies>
            <dependency>
                <groupId>cn.edu.tsinghua</groupId>
                <artifactId>iginx-session</artifactId>
                <version>0.7.0</version>
            </dependency>
        </dependencies>

