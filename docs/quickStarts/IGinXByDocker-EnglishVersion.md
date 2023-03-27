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

Currently, the docker image of IGinX needs to be manually installed locally. First, you need to download the IGinX source code:

```shell
$ cd ~
$ git clone git@github.com:IGinX-THU/IGinX.git # Pull the latest IGinX code
$ cd IGinX
````

Then start building the IGinX image:

Currently, there are two types of IGinx image construction:
- oneShot: All dependencies including ZooKeeper, IGinX and IoTDB can be packaged and run with one shot.
- onlyIginx: Another way is to build the IGinx image separately, requiring the user to manually start the ZooKeeper and IoTDB nodes externally.

## oneShot

Then use the following command to build and run the IGinX image:

```shell
$ cd docker/oneShot
$ ./build_and_run_iginx_docker.sh
````

The following words are displayed to indicate that the image was built and run successfully:

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

## onlyIginx

Use the following command to build the IGinX image:

```shell
$ cd docker/onlyIginx
$ ./build_iginx_docker.sh
````

The following words are displayed to indicate that the image was built successfully:

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

Then start to run the image
Considering that IGinX and IoTDB communicated through the network before, it is necessary to establish a Docker network to allow them to be interconnected through the network. Here we create a bridge network called docker-cluster-iginx:

```shell
$ docker network create -d bridge --attachable --subnet 172.40.0.0/16 docker-cluster-iginx
```

Now start Zookeeper:
```shell
$ cd ${zookeeper_path}
$ ./bin/zkServer.sh start
```

And then start an IoTDB instance:

```shell
$ cd ${iotdb_path}
# ./sbin/start-server.sh
```

Finally, start IGinX, choose to use zookeeper as the metadata storage backend, and set the backend storage as the IoTDB instance just started to complete the startup of the entire system:

```shell
$ cd ${iginx_path}/docker/onlyIginx
$ ./run_iginx_docker.sh 172.40.0.2 10000
```

This command will expose the local 10000 interface as the communication interface with the IGinX cluster. You can start accessing IGinx through 172.40.0.2:8086

Warning: Before starting to build the IGinx image, you need to change the IoTDB and Zookeeper address parameters in IGinx (do not use 127.0.0.1 as the IP parameter)