#
# IGinX - the polystore system with high performance
# Copyright (C) Tsinghua University
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#

FROM maven:3-amazoncorretto-8 AS builder
COPY . /root/iginx
WORKDIR /root/iginx
RUN mvn clean package -pl client -am -Dmaven.test.skip=true -P-format -e

FROM openjdk:11-jre-slim
COPY --from=builder /root/iginx/client/target/iginx-client-0.7.0-SNAPSHOT/ /iginx_client/

RUN mkdir -p /iginx_client/logs
RUN mkdir -p /iginx_client/data
VOLUME /iginx_client/logs
ENV PATH="/iginx_client/sbin/:${PATH}"
ENTRYPOINT ["tail", "-f", "/dev/null"]