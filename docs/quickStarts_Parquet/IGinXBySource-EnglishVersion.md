# IGinX Installation Manual - By Source (Compilation and Installation)

[TOC]

IGinX is an open source polystore system. A polystore system provides an integrated data management service over a set of one or more potentially heterogeneous database/storage engines, serving heterogeneous workloads.

Currently, IGinX directly supports big data service over relational database PostgreSQL, time series databases InfluxDB/IoTDB/TimescaleDB/OpenTSDB, and Parquet data files.

## Download and Installation

### Java Installation

Since ZooKeeper, IGinX and IoTDB are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

1. First, visit the [official Java website](https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download the JDK package for your current system.

2. Installation

```shell
$ cd ~/Downloads
$ tar -zxf jdk-8u181-linux-x64.gz # unzip files
$ mkdir /opt/jdk
$ mv jdk-1.8.0_181 /opt/jdk/
```

3. Set the path

Edit the ~/.bashrc file and add the following two lines at the end of the file:

```shell
export JAVA_HOME = /usr/jdk/jdk-1.8.0_181
export PATH=$PATH:$JAVA_HOME/bin
```

Load the file with the changed configuration (into shell scripts):

```shell
$ source ~/.bashrc
```

4. Use java -version to determine whether JDK installed successfully.

```shell
$ java -version
java version "1.8.0_181"
Java(TM) SE Runtime Environment (build 1.8.0_181-b13)
Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
```

If the words above are displayed, it means the installation was successful.

### Maven Installation

Maven is a build automation tool used primarily to build and managa Java projects. If you need to compile from the source code, you also need to install a Maven environment >= 3.6. Otherwise, **skip this step entirely**.

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

### Node.js Installation

The project requires Node.js installation. If you already have it installed on your device, **skip this part**.

1. Download Node.js package from [here](https://nodejs.org/en/download).

```shell
$ cd ~
$ wget https://nodejs.org/dist/v18.16.1/node-v18.16.1-linux-x64.tar.xz
$ tar -zxvf node-v18.16.1-linux-x64.tar.xz
$ sudo mv -f node-v18.16.1-linux-x64 /usr/local/
```

2. Add environment variable.

Attach code below to file `~/.bashrc`

```shell
export NODEJS_HOME=/usr/local/node-v18.16.1-linux-x64
export PATH=${PATH}:${NODEJS_HOME}/bin
```

Load file `~/.bashrc` to apply changes.

```shell
$ source ~/.bashrc
```

Confirm the Installation by testing `npm` command

```shell
$ npm -v
9.5.1
```

### ZooKeeper Installation

ZooKeeper enables highly reliable distributed coordination. In IGinX, ZooKeeper makes it possible to have more than one IGinX instances deployed.

1. Download ZooKeeper package from [here](https://zookeeper.apache.org/releases.html).

```shell
$ cd ~
$ wget https://dlcdn.apache.org/zookeeper/zookeeper-3.7.2/apache-zookeeper-3.7.2-bin.tar.gz
$ tar -zxvf apache-zookeeper-3.7.2-bin.tar.gz
```

2. Modify config.

```shell
$ cd apache-zookeeper-3.7.2-bin/
$ mkdir data
$ cp conf/zoo_sample.cfg conf/zoo.cfg
```

Modify file`conf/zoo.cfg`.

```shell
# Original conf/zoo.cfg
dataDir=/tmp/zookeeper
# ↓ change to
dataDir=data
```

### IGinX Installation

Compile with source code. If you need to modify code yourself, you can use this installation method.

#### Compilation with source code

Fetch the latest development version and build it locally.

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git
$ cd IGinX
$ mvn clean install -Dmaven.test.skip=true
```

The following words are displayed, indicating that the IGinX build is successful:

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

## Launch

### ZooKeeper

Launch ZooKeeper before IGinX.

```shell
$ cd ~
$ cd apache-zookeeper-3.7.2-bin/
$ ./bin/zkServer.sh start
```

You will see messages as below if ZooKeeper is successfully started.

```shell
ZooKeeper JMX enabled by default
Using config: /home/root/apache-zookeeper-3.7.2-bin/bin/../conf/zoo.cfg
Starting zookeeper ... STARTED
```

### IGinX

Modify `IGINX_HOME/conf/config.properties` and set storage engine to Parquet.

```properties
storageEngineList=127.0.0.1#6667#parquet#dir=parquetData#has_data=false#is_read_only=false
# dir=parquetData will create a /parquetData folder in the working directory. You can customize the name.
```

Using source code to launch

```shell
$ cd ~
$ cd IGinX
$ chmod +x startIginX.sh # enable permissions for startup scripts
$ ./startIginX.sh
```

The following display of words means the IGinX installation was successful：

```shell
2023-06-30 10:32:05,260 [Thread-3]  INFO - [cn.edu.tsinghua.iginx.parquet.server.ParquetServer.startServer:42] parquet service starts successfully!


IGinX is now in service......
```

## Using IGinX

### RESTful Interface

After the startup is complete, you can easily use the RESTful interface to write and query data to IGinX.

Create a file insert.json and add the following into it:

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

Insert data into the database using the following command:

```shell
$ curl -XPOST -H'Content-Type: application/json' -d @insert.json http://127.0.0.1:7888/api/v1/datapoints
```

After inserting data, you can also query the data just written using the RESTful interface.

Create a file query.json and write the following data into it:

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

Enter the following command to query the data:

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

If you see the following information returned, it means you are able to successfully use RESTful interface to write and query data to IGinX.

For more interfaces, please refer to the official [IGinX manual](../pdf/userManualC.pdf).

If you want to use a different interface, there is another option.

In addition to the RESTful interface, IGinX also provides the RPC data access interface. For this specific interface, please refer to the official [IGinX manual](../pdf/userManualC.pdf).

At the same time, IGinX also provides some official examples, showing the most common usage of the RPC interface.

Below is a short tutorial on how to use it.

### RPC Interface

Since the IGinX jars have not been released to the Maven central repository, if you want to use it, you need to manually install it to the local Maven repository.

The specific installation method is as follows:

```shell
# download the newest iginx release version source code package
$ wget https://github.com/IGinX-THU/IGinX/releases/download/v0.7.0/IGinX-FastDeploy-0.7.0.tar.gz
# Unzip the source package
$ tar -zxvf v0.7.0.tar.gz
# go to the main project's directory
$ cd IGinX-rc-v0.7.0
# Install to local Maven repository
$ mvn clean install -DskipTests
```

Specifically, when using it, you only need to introduce the following dependencies in the pom file of the corresponding project:

```xml
<dependency>
    <groupId>cn.edu.tsinghua</groupId>
    <artifactId>iginx-core</artifactId>
    <version>0.7.0</version>
</dependency>
```

Before accessing IGinX, you first need to create a session and try to connect. The Session constructor has 4 parameters, which are the ip and port IGinX will to connect to, and the username and password for IGinX authentication. The current authentication system is still being written, so the account name and password to access the backend IGinX can directly fill in root:

```Java
Session session = new Session("127.0.0.1", 6888, "root", "root");
session.openSession();
```

You can then try to insert data into IGinX. Since IGinX supports the creation of time-series when data is written for the first time, there is no need to call the relevant series creation interface in advance. IGinX provides row-style and column-style data writing interfaces.

The following is an example of using the column-style data writing interface:

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

After the session is completed, you need to manually close and release your connection from your terminal/backend:

```shell
session.closeSession();
```

For the full version of the code, please refer to: https://github.com/IGinX-THU/IGinX/blob/main/example/src/main/java/cn/edu/tsinghua/iginx/session/ParquetSessionExample.java

## FAQ

### Unable to find parquet-file dependency

When compiling IGinX with Maven, you may encounter the following error:

```
The following artifacts could not be resolved: cn.edu.tsinghua.iginx:parquet-file
```

This is because the parquet-file dependency required by IGinX is not published to the Maven Central Repository but is hosted on GitHub Pages. Therefore, we need to add the following repository address to the pom.xml file:

```xml
<repositories>
    <repository>
        <id>parquet-file</id>
        <name>IGinX GitHub repository</name>
        <url>https://iginx-thu.github.io/Parquet/maven-repo</url>
    </repository>
</repositories>
```

If you have configured a mirror, for example:

```xml
<mirror>
    <id>aliyunmaven</id>
    <mirrorof>central</mirrorOf>
    <name>Aliyun Public Repository</name>
    <url>https://maven.aliyun.com/repository/central</url>
</mirror>
```

Note that the value of mirrorOf is central, which means that only when accessing the Maven Central Repository will the Aliyun mirror be used.
If you configure it as *, it means that all Maven repositories will use the Aliyun mirror, which will cause the parquet-file dependency required
by IGinX to fail to download.

If you are unable to download the dependency due to network issues, you can try using a proxy.
Additionally, you can manually download the dependency and then use Maven's install command to install it to
the local repository. For more details, see: https://maven.apache.org/guides/mini/guide-3rd-party-jars-local.html
