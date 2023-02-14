# IGinX Installation and Use Manual (By Docker)

IGinX is an open source polystore system. A polystore system provides an integrated data management service over a set of one or more potentially heterogeneous database/storage engines, serving heterogeneous workloads.

Currently, IGinX directly supports big data service over relational database PostgreSQL, time series databases InfluxDB/IoTDB/TimescaleDB/OpenTSDB, and Parquet data files.

## Environment Installation

### Java Installation

Since ZooKeeper, IGinX and IoTDB are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

1. First, visit the [official Java website] (https://www.oracle.com/java/technologies/javase/javase-jdk8-downloads.html) to download the JDK package for your current system.

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

### Docker installation

Docker provides an official installation script that allows users to automatically install it by entering the command:

```shell
$ curl -fsSL https://get.docker.com | bash -s docker --mirror Aliyun
````

You can also use the internal daocloud one-click installation command:

```shell
$ curl -sSL https://get.daocloud.io/docker | sh
````

Run this command to start the docker engine and check the docker version:

```shell
$ systemctl start docker
$ docker version
````

If the words above are displayed, it means the installation was successful:

```shell
Client: Docker Engine - Community
 Version: 20.10.8
 API version: 1.41
 Go version: go1.16.6
 Git commit: 3967b7d
 Built: Fri Jul 30 19:55:49 2021
 OS/Arch: linux/amd64
 Context: default
 Experimental: true

Server: Docker Engine - Community
 Engine:
  Version: 20.10.8
  API version: 1.41 (minimum version 1.12)
  Go version: go1.16.6
  Git commit: 75249d8
  Built: Fri Jul 30 19:54:13 2021
  OS/Arch: linux/amd64
  Experimental: false
 containerd:
  Version: 1.4.9
  GitCommit: e25210fe30a0a703442421b0f60afac609f950a3
 runc:
  Version: 1.0.1
  GitCommit: v1.0.1-0-g4144b63
 docker-init:
  Version: 0.19.0
  GitCommit: de40ad0
````

## Compile the Image

Currently, the docker image of IGinX needs to be manually installed locally. First, you need to download the IGinX source code and compile it:

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git # Pull the latest IGinX code
$ cd IGinX
$ mvn clean install -Dmaven.test.skip=true # Compile IGinX
````

If the words above are displayed, it means the installation was successful:

```shell
[INFO] --------------------------------------------------------- -------------------------
[INFO] Reactor Summary for IGinX 0.6.0-SNAPSHOT:
[INFO]
[INFO] IGinX ................................................ SUCCESS [0.274s]
[INFO] IGinX Thrift ............................................... SUCCESS [ 6.484 s]
[INFO] IGinX Shared ............................................... SUCCESS [ 1.015 s]
[INFO] IGinX Session ............................................... SUCCESS [ 0.713 s]
[INFO] IGinX Antlr ............................................... SUCCESS [ 1.654 s]
[INFO] IGinX Core ............................................... SUCCESS [ 9.471 s ]
[INFO] IGinX IoTDB ................................................ SUCCESS [ 1.234 s]
[INFO] IGinX InfluxDB ............................................... SUCCESS [ 0.823 s]
[INFO] IGinX Client ............................................... SUCCESS [ 3.045 s]
[INFO] IGinX JDBC ............................................... SUCCESS [ 0.802 s ]
[INFO] IGinX Example ............................................... SUCCESS [ 0.606 s]
[INFO] IGinX Test ............................................... SUCCESS [ 0.116 s ]
[INFO] --------------------------------------------------------- -------------------------
[INFO] BUILD SUCCESS
[INFO] --------------------------------------------------------- -------------------------
[INFO] Total time: 26.432 s
[INFO] Finished at: 2021-08-19T11:06:12+08:00
[INFO] --------------------------------------------------------- -------------------------

````

Then use the maven plugin to build the IGinX image:

```shell
$ mvn package shade:shade -pl core -DskipTests docker:build
````

The following words are displayed to indicate that the image was built successfully:

```shell
Successfully tagged iginx:latest
[INFO] Built iginx
[INFO] --------------------------------------------------------- -------------------------
[INFO] BUILD SUCCESS
[INFO] --------------------------------------------------------- -------------------------
[INFO] Total time: 21.268 s
[INFO] Finished at: 2021-08-19T11:08:46+08:00
[INFO] --------------------------------------------------------- -------------------------
````

You can use the docker command to view the IGinX image that has been installed locally:

```shell
$ docker images
REPOSITORY TAG IMAGE ID CREATED SIZE
iginx latest 7f00b2b9510b 36 seconds ago 702MB
```

## Mirror-based operation

First you need to pull the IoTDB image from DockerHub:

```shell
$ docker pull apache/iotdb:0.11.4
0.11.4: Pulling from apache/iotdb
a076a628af6f: Pull complete
943d8acaac04: Downloading
b9998d19c116: Download complete
8162353a3d61: Waiting
af85646a1131: Waiting
0.11.4: Pulling from apache/iotdb
a076a628af6f: Pull complete
943d8acaac04: Pull complete
b9998d19c116: Pull complete
8162353a3d61: Pull complete
af85646a1131: Pull complete
Digest: sha256:a29a48297e6785ac13abe63b4d06bdf77d50203c13be98b773b2cfb07b76d135
Status: Downloaded newer image for apache/iotdb:0.11.4
docker.io/apache/iotdb:0.11.4
```

Considering that IGinX and IoTDB communicated through the network before, it is necessary to establish a Docker network to allow them to be interconnected through the network. Here we create a bridge network called iot:

```shell
$ docker network create -d bridge iot
```

Now start an IoTDB instance:

```shell
$ docker run -d --name iotdb --network iot apache/iotdb:0.11.4
```

Finally, start IGinX, choose to use a local file as the metadata storage backend, and set the backend storage as the IoTDB instance just started to complete the startup of the entire system:

```shell
$ docker run -e "metaStorage=file" -e "storageEngineList=iotdb#6667#iotdb#sessionPoolSize=20" -p 6888:6888 --network iot -it iginx
```

This command will expose the local 6888 port as the communication port to interface with the IGinX cluster.

After the system is started, you can run the cn.edu.tsinghua.iginx.session.IoTDBSessionExample class in the example directory of the IGinX source code, and try to insert/read data into it.

> Parameter settings for the IGinX docker version:
>
> Normally, IGinX uses a local file as the source of configuration parameters. However, in the latest version of the main branch, environment variables are supported to configure parameters.
>
> For example, the parameter port in the configuration file represents the startup port of the system, which is specified as 6888 in the configuration file. Meaning the port exposed by IGinX to the outside world is 6888.
>
> ```shell
> port=6888
> ```
> 
> You can set the environment variable port to other values, such as 6889. In that case, IGinX's startup port will be changed to 6889.
>
> ```shell
> export port=6889
> ```
>
> Parameters set in environment variables override parameters in the configuration file.
>
> In the Docker environment, the way to change the configuration file is cumbersome, so it is recommended to use the method of setting the Docker container environment variable to configure the system parameters.