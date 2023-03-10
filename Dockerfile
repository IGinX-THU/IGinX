FROM maven:3-amazoncorretto-8 AS builder
COPY . /root/IGinX
WORKDIR /root/IGinX
RUN --mount=type=cache,target=/root/.m2 mvn clean package -pl core,dataSources/iotdb12 -am -Dmaven.test.skip=true -Drevision=dev

FROM amazoncorretto:8
COPY --from=builder /root/IGinX/core/target/iginx-core-dev /root/IGinX
COPY docker/config.properties /root/IGinX/conf/config.properties
ENV PS1='\u@\h:\w\$ '
