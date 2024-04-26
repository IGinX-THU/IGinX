#FROM maven:3-amazoncorretto-8 AS builder
#COPY . /root/iginx
#WORKDIR /root/iginx
#RUN mvn clean package -pl client -am -Dmaven.test.skip=true -P-format -e

FROM openjdk:11-jre-slim
#COPY . /root/iginx
#WORKDIR /root/iginx
#COPY --from=builder /root/iginx/client/target/iginx-client-0.6.0-SNAPSHOT/ /iginx_client/
COPY ./client/target/iginx-client-0.6.0-SNAPSHOT/ /iginx_client/

RUN mkdir -p /iginx_client/logs
RUN mkdir -p /iginx_client/data
VOLUME /iginx_client/logs
ENV PATH="/iginx_client/sbin/:${PATH}"
ENTRYPOINT ["tail", "-f", "/dev/null"]