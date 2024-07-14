# IGinX 安装使用教程（Docker运行）

IGinX是清华大学“清华数为”大数据软件栈的“大数据总线”，面向解决用户在大数据场景下“管数烦、用数难”的问题而研发。它的特色包括“负载均衡弹性好、异构关联全局化、数据使用不搬家、Python集成便利大、SQL输入实时查”。

IGinX支持用户一体化管理已存储在不同系统中的数据资产，也支持用户统一读写、查询、关联特定系统中的数据。目前，IGinX支持一体化管理包括关系数据库PostgreSQL、时序数据库InfluxDB/IotDB/TimescaleDB/OpenTSDB、大数据文件Parquet集合等存储的数据。

## 环境安装

### Java 安装

由于 ZooKeeper、IGinX 都是使用 Java 开发的，因此首先需要安装 Java。如果本地已经安装了 JDK>=1.8 的运行环境，**直接跳过此步骤**。

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

### Docker 安装

docker 官方提供了安装脚本，允许用户使用命令自动安装：

```shell
$ curl -fsSL https://get.docker.com | bash -s docker --mirror Aliyun
```

也可以使用国内 daocloud 一键安装命令：

```shell
$ curl -sSL https://get.daocloud.io/docker | sh
```

运行命令即可启动 docker engine，并查看 docker 版本：

```shell
$ systemctl start docker
$ docker version
```

显示出如下字样，表示 docker 安装成功：

```shell
Client: Docker Engine - Community
 Version:           20.10.8
 API version:       1.41
 Go version:        go1.16.6
 Git commit:        3967b7d
 Built:             Fri Jul 30 19:55:49 2021
 OS/Arch:           linux/amd64
 Context:           default
 Experimental:      true

Server: Docker Engine - Community
 Engine:
  Version:          20.10.8
  API version:      1.41 (minimum version 1.12)
  Go version:       go1.16.6
  Git commit:       75249d8
  Built:            Fri Jul 30 19:54:13 2021
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.4.9
  GitCommit:        e25210fe30a0a703442421b0f60afac609f950a3
 runc:
  Version:          1.0.1
  GitCommit:        v1.0.1-0-g4144b63
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

## 编译镜像

目前 IGinX 的 docker 镜像需要手动安装到本地。首先需要下载 IGinX 源码：

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git # 拉取最新的 IGinX 代码
$ cd IGinX
```

随后开始构建 IGinX 镜像：

目前 IGinX 镜像的构建分为两种：
- oneShot: 可以将包括 ZooKeeper、IGinX 在内所有依赖进行一键打包及运行。
- onlyIginx: 另一种为单独构建 IGinX 镜像，需要用户在外部手动启动 ZooKeeper。

## oneShot 镜像

oneShot方法中，将ZooKeeper、IGinX分别打包为镜像运行，并创建一个网络以便服务之间进行通信。

在打包之前，需要修改`conf/config.properties`文件，修改IGinX中有关Zookeeper的参数设置，将用于访问服务的ip地址改为ZooKeeper容器的hostname，否则IGinX将无法访问Zookeeper服务。

```properties
# ZooKeeper hostname: zkServer
zookeeperConnectionString=zkServer:2181
```

Zookeeper的hostname可以在`$IGINX_HOME/docker/oneShot/docker-compose.yaml`中进行自定义：

```yaml
services:
  zookeeper:
    hostname: "custom_zookeeper_hostname"	# default: zkServer
# conf/config.properties 需要做相应的修改
```

修改完成后，运行以下命令进行镜像的构建和运行：

```shell
$ cd docker/oneShot
$ ./build_and_run_iginx_docker.sh
```

显示出如下的字样表示镜像构建成功并且成功启动：

```shell
[+] Building 18.2s (12/12) FINISHED
 => [iginx internal] load build definition from Dockerfile                                                         0.0s
 => => transferring dockerfile: 387B                                                                               0.0s
 => [iginx internal] load .dockerignore                                                                            0.0s
 => => transferring context: 2B                                                                                    0.0s
 => [iginx internal] load metadata for docker.io/library/amazoncorretto:8                                          1.0s
 => [iginx internal] load metadata for docker.io/library/maven:3-amazoncorretto-8                                  1.1s
 => [iginx internal] load build context                                                                            0.3s
 => => transferring context: 465.28kB                                                                              0.2s
 => CACHED [iginx stage-1 1/2] FROM docker.io/library/amazoncorretto:8@sha256:39679cbf42bf0ac2f8de74aa2e87f162eb1  0.0s
 => [iginx builder 1/4] FROM docker.io/library/maven:3-amazoncorretto-8@sha256:8891ab4c3fe9beb924af0595eef2389d24  0.0s
 => CACHED [iginx builder 2/4] COPY . /root/IGinX                                                                  0.0s
 => CACHED [iginx builder 3/4] WORKDIR /root/IGinX                                                                 0.0s
 => [iginx builder 4/4] RUN --mount=type=cache,target=/root/.m2 mvn clean package -pl core,dataSources/parquet -  15.8s
 => [iginx stage-1 2/2] COPY --from=builder /root/IGinX/core/target/iginx-core-dev /root/IGinX                     0.2s
 => [iginx] exporting to image                                                                                     0.4s
 => => exporting layers                                                                                            0.4s
 => => writing image sha256:3754b479bb230728b9b25194248f8dff1348ba87bd5925ddc91cc32fcdc5a0ee                       0.0s
 => => naming to docker.io/library/oneshot-parquet-iginx                                                           0.0s
[+] Running 3/3
 ✔ Network oneshot-parquet_net  Created                                                                            0.6s
 ✔ Container zookeeper          Started                                                                            1.0s
 ✔ Container iginx1             Started
```

开启容器后，可以通过127.0.0.1:10001访问IGinX服务。

## onlyIginx 镜像

**注：在开始构建镜像前需要把 IGinX 配置的网络地址参数进行更改，将`conf/config.properties`中所有的“127.0.0.1”更改为“host.docker.internal”，以便IGinX镜像与宿主机的ZooKeeper和数据库进程进行通信**

对于 onlyIginx 镜像，其构建方法如下：

```shell
$ cd docker/onlyIginx
$ ./build_iginx_docker.sh 
```

显示出如下的字样表示镜像构建成功：

```shell
[+] Building 887.9s (12/12) FINISHED
=> [internal] load build definition from Dockerfile-iginx                                                         0.1s
=> => transferring dockerfile: 406B                                                                               0.0s
=> [internal] load .dockerignore                                                                                  0.1s
=> => transferring context: 2B                                                                                    0.0s
=> [internal] load metadata for docker.io/library/openjdk:11-jre-slim                                             5.3s
=> [internal] load metadata for docker.io/library/maven:3-amazoncorretto-8                                        3.3s
=> [internal] load build context                                                                                  3.6s
=> => transferring context: 475.61MB                                                                              3.5s
=> CACHED [builder 1/4] FROM docker.io/library/maven:3-amazoncorretto-8@sha256:c9d6016fad9c479b874f270a80d80f891  0.0s
=> [stage-1 1/2] FROM docker.io/library/openjdk:11-jre-slim@sha256:93af7df2308c5141a751c4830e6b6c5717db102b3b31  38.3s
=> => resolve docker.io/library/openjdk:11-jre-slim@sha256:93af7df2308c5141a751c4830e6b6c5717db102b3b31f012ea29d  0.1s
=> => sha256:764a04af3eff09cc6a29bcc19cf6315dbea455d7392c1a588a5deb331a929c29 7.55kB / 7.55kB                     0.0s
=> => sha256:1efc276f4ff952c055dea726cfc96ec6a4fdb8b62d9eed816bd2b788f2860ad7 31.37MB / 31.37MB                  25.6s
=> => sha256:a2f2f93da48276873890ac821b3c991d53a7e864791aaf82c39b7863c908b93b 1.58MB / 1.58MB                     2.7s
=> => sha256:12cca292b13cb58fadde25af113ddc4ac3b0c5e39ab3f1290a6ba62ec8237afd 212B / 212B                         1.2s
=> => sha256:93af7df2308c5141a751c4830e6b6c5717db102b3b31f012ea29d842dc4f2b02 549B / 549B                         0.0s
=> => sha256:884c08d0f406a81ae1b5786932abaf399c335b997da7eea6a30cc51529220b66 1.16kB / 1.16kB                     0.0s
=> => sha256:d73cf48caaac2e45ad76a2a9eb3b311d0e4eb1d804e3d2b9cf075a1fa31e6f92 46.04MB / 46.04MB                  37.3s
=> => extracting sha256:1efc276f4ff952c055dea726cfc96ec6a4fdb8b62d9eed816bd2b788f2860ad7                          1.0s
=> => extracting sha256:a2f2f93da48276873890ac821b3c991d53a7e864791aaf82c39b7863c908b93b                          0.1s
=> => extracting sha256:12cca292b13cb58fadde25af113ddc4ac3b0c5e39ab3f1290a6ba62ec8237afd                          0.0s
=> => extracting sha256:d73cf48caaac2e45ad76a2a9eb3b311d0e4eb1d804e3d2b9cf075a1fa31e6f92                          0.7s
=> [builder 2/4] COPY . /root/iginx                                                                               1.3s
=> [builder 3/4] WORKDIR /root/iginx                                                                              0.1s
=> [builder 4/4] RUN mvn clean package -DskipTests -P passFormat                                                876.3s
=> [stage-1 2/2] COPY --from=builder /root/iginx/core/target/iginx-core-0.8.0-SNAPSHOT /iginx                     0.2s
=> exporting to image                                                                                             0.5s
=> => exporting layers                                                                                            0.5s
=> => writing image sha256:e738348598c9db601dbf39c7a8ca9e1396c5ff51769afeb0fe3da12e2fdcd73a                       0.0s
=> => naming to docker.io/library/iginx:0.6.0
```

接下来运行镜像，创建IGinX容器。

首先启动 Zookeeper：

```shell
$ cd ${zookeeper_path}
$ ./bin/zkServer.sh start
```

然后对IGinX配置文件`$IGINX_HOME/conf/config.properties`进行修改：

1. IGinX IP:

   a. 如果不会给IGinX容器指定IP，则填写宿主机的IP。

   ```properties
   # example:
   ip:11.272.81.34
   ```

   b. 如果需要给IGinX指定IP，则填写将要分配的IP。

   ```properties
   # example
   ip:172.34.1.2
   ```
2. Parquet数据库服务的IP:

   a. 如果需要让该IGinX启动一个本地parquet服务，则为Parquet服务配置和IGinX相同的IP。

   ```properties
   # example
   ip:11.272.81.34
   storageEngineList=11.272.81.34#6667#parquet#dir=/path/to/your/parquet#dummy_dir=/path/to/your/data#iginx_port=6888#has_data=false#is_read_only=false
   ```

   b. 如果需要连接一个已存在的远程Parquet服务，则配置相应的IP地址

   ```properties
   # example
   ip:11.272.81.34
   storageEngineList=11.272.83.2#6667#parquet#dir=/path/to/your/parquet#dummy_dir=/path/to/your/data#iginx_port=6888#has_data=false#is_read_only=false
   ```
3. ZooKeeper服务的IP:

   填写ZooKeeper服务实际运行在的宿主机的IP地址。

   若ZooKeeper是作为容器运行，且和该IGinX容器在同一个网桥，可以使用该ZooKeeper容器的hostname。

   ```properties
   # example
   zookeeperConnectionString=11.272.81.34:2181
   ```

最后启动 IGinX即可完成整个系统的启动：

```shell
$ cd ${iginx_path}/docker/onlyIginx
$ ./run_iginx_docker.sh -n iginx0 -p 10001
# -n 该IGinX容器的名字
# -p 10001为IGinX容器映射到宿主机的端口，用户可以根据自己的主机情况自定义
```

该命令会将本地的 10001 接口暴露出来，作为与 IGinX 集群的通讯接口。在宿主机上通过地址 127.0.0.1:10001 即可开始访问 IGinX。

运行脚本同时提供了两个可选的命令行参数 `-c` 和 `-o`。用户可以通过配置这两个参数进一步控制IGinX容器的运行方式。

```shell
# example
$ ./run_iginx_docker.sh -n iginx0 -p 10001 -c my/path/to/config -o myNetwork
# -n 该IGinX容器的名字
# -p 10001为IGinX容器映射到宿主机的端口，用户可以根据自己的主机情况自定义
# [optional] -c 指定使用配置文件的绝对地址 (默认值: "$IGINX_HOME/conf/config.properties")
#				(当需要维护多份配置文件时会非常有用)
# [optional] -o overlay network
#				(overlay网络可以进行跨主机的容器通信，能够实现大规模的容器集群)
```

