# IGinX Installation and Use Manual (By Docker)

IGinX is an open source polystore system. A polystore system provides an integrated data management service over a set of one or more potentially heterogeneous database/storage engines, serving heterogeneous workloads.

Currently, IGinX directly supports big data service over relational database PostgreSQL, time series databases InfluxDB/IoTDB/TimescaleDB/OpenTSDB, and Parquet data files.

## Environment Installation

### Java Installation

Since ZooKeeper, IGinX are all developed using Java, Java needs to be installed first. If a running environment of JDK >= 1.8 has been installed locally, **skip this step entirely**.

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

### Docker installation

Docker provides an official installation script that allows users to automatically install it by entering the command:

```shell
$ curl -fsSL https://get.docker.com | bash -s docker --mirror Aliyun
```

You can also use the internal daocloud one-click installation command:

```shell
$ curl -sSL https://get.daocloud.io/docker | sh
```

Run this command to start the docker engine and check the docker version:

```shell
$ systemctl start docker
$ docker version
```

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
```

## Compile the Image

Currently, the docker image of IGinX needs to be manually installed locally. First, you need to download the IGinX source code:

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git # Pull the latest IGinX code
$ cd IGinX
```

Then start building the IGinX image:

Currently, there are two types of IGinX image construction:
- oneShot: All dependencies including ZooKeeper, IGinX can be packaged and run with one shot.
- onlyIginx: Another way is to build the IGinX image separately, requiring the user to manually start the ZooKeeper service externally.

## oneShot

In this method, ZooKeeper and IGinX run as 2 separate containers with a custom network bridge which enables IGinX to communicate with ZooKeeper.

Before building the image, the params in `conf/config.properties` need to be changed. The IP address for ZooKeeper should be changed to the hostnames of ZooKeeper container. Otherwise, IGinX will not be able to access it.

```properties
# ZooKeeper hostname: zkServer
zookeeperConnectionString=zkServer:2181
```

Hostname for Zookeeper container can be changed in `$IGINX_HOME/docker/oneShot-parquet/docker-compose.yaml`：

```yaml
services:
  zookeeper:
    hostname: "custom_zookeeper_hostname"	#default: zkServer
# conf/config.properties should be changed accordingly
```

Then use the following command to build image and run:

```shell
$ cd docker/oneShot
$ ./build_and_run_iginx_docker.sh
```

The following words are displayed to indicate that the image was built and run successfully:

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

After this step, 127.0.0.1:10001 will be accessable for IGinX service.

## onlyIginx

**Warning: Before starting to build the IGinX image, you need to modify the network address parameters in IGinX. All "127.0.0.1" should be replaced by "host.docker.internal" to enable the IGinX container to communicate with ZooKeeper service that runs on host machine.**

Use the following command to build the IGinX image:

```shell
$ cd docker/onlyIginx
$ ./build_iginx_docker.sh
```

The following words are displayed to indicate that the image was built successfully:

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
=> [stage-1 2/2] COPY --from=builder /root/iginx/core/target/iginx-core-0.8.0 /iginx                     0.2s
=> exporting to image                                                                                             0.5s
=> => exporting layers                                                                                            0.5s
=> => writing image sha256:e738348598c9db601dbf39c7a8ca9e1396c5ff51769afeb0fe3da12e2fdcd73a                       0.0s
=> => naming to docker.io/library/iginx:0.6.0
```

Then start to run the image.

Start Zookeeper first:

```shell
$ cd ${zookeeper_path}
$ ./bin/zkServer.sh start
```

Then, modify IGinX configuration file `$IGINX_HOME/conf/config.properties`:

1. IP for IGinX:

   a. If you are **not** giving the container a customized IP, fill with IP address of the host server.

   ```properties
   # example:
   ip:11.272.81.34
   ```

   b. If you are going to giving the container a customized IP, fill with this customized IP address.

   ```properties
   # example
   ip:172.34.1.2
   ```
2. IP for parquet server:

   a. If you want IGinX to start a parquet database service, IP for parquet database server should be the same as IP for IGinX.

   ```properties
   # example
   ip:11.272.81.34
   storageEngineList=11.272.81.34#6667#parquet#dir=/path/to/your/parquet#dummy_dir=/path/to/your/data#iginx_port=6888#has_data=false#is_read_only=false
   ```

   b. If you want to connect to a remote parquet server, use it's IP address.

   ```properties
   # example
   ip:11.272.81.34
   storageEngineList=11.272.83.2#6667#parquet#dir=/path/to/your/parquet#dummy_dir=/path/to/your/data#iginx_port=6888#has_data=false#is_read_only=false
   ```
3. IP for ZooKeeper service:

   You should use the real IP address of the host server that ZooKeeper service runs on. Or, if ZooKeeper runs as a container with IGinX containers in the same net bridge, you can choose ZooKeeper container's hostname instead.

   ```properties
   # example
   zookeeperConnectionString=11.272.81.34:2181
   ```

Finally, start IGinX to complete the startup of the entire system:

```shell
$ cd ${iginx_path}/docker/onlyIginx
$ ./run_iginx_docker.sh -n iginx0 -p 10001
# -n container name
# -p host port to cast
```

This command will expose the host's port 10001 as the communication interface with the IGinX cluster. You can start accessing IGinX through 127.0.0.1:10001.

We also provide optional CMD parameters `-c` and `-o` for users to customize how IGinX container would run:

```shell
# example
$ ./run_iginx_docker.sh -n iginx0 -p 10001 -c my/path/to/config -o myNetwork
# -n container name
# -p host port to cast
# [optional] -c absolute path to config file (default: "$IGINX_HOME/conf/config.properties")
#				(very helpful to keep different versions of configuration files)
# [optional] -o overlay network
#				(to connect different containers from different hosts)
```

