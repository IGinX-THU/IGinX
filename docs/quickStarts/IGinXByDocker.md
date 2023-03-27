# IGinX 安装使用教程（Docker运行）

IGinX是清华大学“清华数为”大数据软件栈的“大数据总线”，面向解决用户在大数据场景下“管数烦、用数难”的问题而研发。它的特色包括“负载均衡弹性好、异构关联全局化、数据使用不搬家、Python集成便利大、SQL输入实时查”。

IGinX支持用户一体化管理已存储在不同系统中的数据资产，也支持用户统一读写、查询、关联特定系统中的数据。目前，IGinX支持一体化管理包括关系数据库PostgreSQL、时序数据库InfluxDB/IotDB/TimescaleDB/OpenTSDB、大数据文件Parquet集合等存储的数据。

## 环境安装

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

目前 IGinX 的 docker 镜像需要手动安装到本地。首先需要下载 Iginx 源码：

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git # 拉取最新的 IGinX 代码
$ cd IGinX
```

随后开始构建 IGinX 镜像：

目前 IGinx 镜像的构建分为两种：
- oneShot: 可以将包括 ZooKeeper、IGinX 以及 IoTDB 在内所有依赖进行一键打包及运行。
- onlyIginx: 另一种为单独构建 IGinx 镜像，需要用户在外部手动启动 ZooKeeper 和 IoTDB 节点。

## oneShot 镜像

对于 oneShot 镜像，其构建方法并运行如下：
```shell
$ cd docker/oneShot
$ ./build_and_run_iginx_docker.sh
```

显示出如下的字样表示镜像构建成功并且成功启动：

```shell
[+] Building 2.0s (12/12) FINISHED                                                                                                                                                                                                                                       
 => [internal] load build definition from Dockerfile                                                                                                                                                                                                                0.0s
 => => transferring dockerfile: 32B                                                                                                                                                                                                                                 0.0s
 => [internal] load .dockerignore                                                                                                                                                                                                                                   0.0s
 => => transferring context: 2B                                                                                                                                                                                                                                     0.0s
 => [internal] load metadata for docker.io/library/amazoncorretto:8                                                                                                                                                                                                 1.1s
 => [internal] load metadata for docker.io/library/maven:3-amazoncorretto-8                                                                                                                                                                                         1.1s
 => [builder 1/4] FROM docker.io/library/maven:3-amazoncorretto-8@sha256:38be03b00a04502751725aabcdf40e8b74711b5f5a19a4b5cadcbcc6362761a0                                                                                                                           0.0s
 => => resolve docker.io/library/maven:3-amazoncorretto-8@sha256:38be03b00a04502751725aabcdf40e8b74711b5f5a19a4b5cadcbcc6362761a0                                                                                                                                   0.0s
 => [stage-1 1/2] FROM docker.io/library/amazoncorretto:8@sha256:0b713bebcce236a89ed36afaa2542a68d215045cbbfd391b749190f3218d9b0d                                                                                                                                   0.0s
 => [internal] load build context                                                                                                                                                                                                                                   0.7s
 => => transferring context: 875.73kB                                                                                                                                                                                                                               0.6s
 => CACHED [builder 2/4] COPY . /root/IGinX                                                                                                                                                                                                                         0.0s
 => CACHED [builder 3/4] WORKDIR /root/IGinX                                                                                                                                                                                                                        0.0s
 => CACHED [builder 4/4] RUN --mount=type=cache,target=/root/.m2 mvn clean package -pl core,dataSources/iotdb12 -am -Dmaven.test.skip=true -Drevision=dev                                                                                                           0.0s
 => CACHED [stage-1 2/2] COPY --from=builder /root/IGinX/core/target/iginx-core-dev /root/IGinX                                                                                                                                                                     0.0s
 => exporting to image                                                                                                                                                                                                                                              0.0s
 => => exporting layers                                                                                                                                                                                                                                             0.0s
 => => writing image sha256:c112090e67e2904e1656e3f22588a0456755443c73bb56dd31c1194c3b60ddf3                                                                                                                                                                        0.0s
 => => naming to docker.io/library/oneshot_iginx                                                                                                                                                                                                                    0.0s

Use 'docker scan' to run Snyk tests against images to find vulnerabilities and learn how to fix them
[+] Running 3/3
 ⠿ Container iotdb      Started                                                                                                                                                                                                                                     0.7s
 ⠿ Container zookeeper  Started                                                                                                                                                                                                                                     0.7s
 ⠿ Container iginx1     Started                                                                                                                                                                                                                                     1.6s
```

## onlyIginx 镜像

对于 oneShot 镜像，其构建方法如下：
```shell
$ cd docker/onlyIginx
$ ./build_iginx_docker.sh
```

显示出如下的字样表示镜像构建成功：

```shell
[+] Building 578.0s (12/12) FINISHED                                                                                                                                                                                                                                     
=> [internal] load build definition from Dockerfile-iginx                                                                                                                                                                                                          0.0s
=> => transferring dockerfile: 400B                                                                                                                                                                                                                                0.0s
=> [internal] load .dockerignore                                                                                                                                                                                                                                   0.0s
=> => transferring context: 2B                                                                                                                                                                                                                                     0.0s
=> [internal] load metadata for docker.io/library/openjdk:11-jre-slim                                                                                                                                                                                              1.2s
=> [internal] load metadata for docker.io/library/maven:3-amazoncorretto-8                                                                                                                                                                                         1.2s
=> [internal] load build context                                                                                                                                                                                                                                  10.7s
=> => transferring context: 271.34MB                                                                                                                                                                                                                              10.5s
=> CACHED [builder 1/4] FROM docker.io/library/maven:3-amazoncorretto-8@sha256:38be03b00a04502751725aabcdf40e8b74711b5f5a19a4b5cadcbcc6362761a0                                                                                                                    0.0s
=> => resolve docker.io/library/maven:3-amazoncorretto-8@sha256:38be03b00a04502751725aabcdf40e8b74711b5f5a19a4b5cadcbcc6362761a0                                                                                                                                   0.0s
=> CACHED [stage-1 1/2] FROM docker.io/library/openjdk:11-jre-slim@sha256:93af7df2308c5141a751c4830e6b6c5717db102b3b31f012ea29d842dc4f2b02                                                                                                                         0.0s
=> [builder 2/4] COPY . /root/iginx                                                                                                                                                                                                                                2.6s
=> [builder 3/4] WORKDIR /root/iginx                                                                                                                                                                                                                               0.0s
=> [builder 4/4] RUN ls;mvn clean package -DskipTests                                                                                                                                                                                                            560.0s
=> [stage-1 2/2] COPY --from=builder /root/iginx/core/target/iginx-core-0.6.0-SNAPSHOT /iginx                                                                                                                                                                      1.1s
=> exporting to image                                                                                                                                                                                                                                              1.4s
=> => exporting layers                                                                                                                                                                                                                                             1.3s
=> => writing image sha256:bca6377f80dab1689d6cc9975c2db50046722931edc4e314a1aecceb78833204                                                                                                                                                                        0.0s
=> => naming to docker.io/library/iginx:0.6.0                                                                                                                                                                                                                      0.0s
```

接下来开始运行镜像
考虑到 IGinX 和 IoTDB 之前通过网络进行通讯，因此需要建立 Docker 网络，允许其通过网络互联。在这里我们创建一个名为 docker-cluster-iginx 的 bridge 网络：

```shell
$ docker network create -d bridge --attachable --subnet 172.40.0.0/16 docker-cluster-iginx
```

然后启动 Zookeeper：

```shell
$ cd ${zookeeper_path}
$ ./bin/zkServer.sh start
```

然后启动一个 IoTDB 实例：

```shell
$ cd ${iotdb_path}
# ./sbin/start-server.sh
```

最后启动 IGinX，选择使用 zookeeper 作为元数据存储后端，并设置后端存储为刚刚启动的 IoTDB 实例即可完成整个系统的启动：

```shell
$ cd ${iginx_path}/docker/onlyIginx
$ ./run_iginx_docker.sh 172.40.0.2 10000
```

该命令会将本地的 10000 接口暴露出来，作为与 IGinX 集群的通讯接口。通过地址 172.40.0.2:8086 即可开始访问 IGinx

注：在开始构建镜像前需要把 IGinx 中的 IoTDB 和 Zookeeper 地址参数进行更改（请勿使用 127.0.0.1 作为 IP 参数）
