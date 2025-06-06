/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestServer implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(RestServer.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private static URI baseURI;

  private static URI getbaseuri() {
    return UriBuilder.fromUri("http://" + config.getRestIp() + "/")
        .port(config.getRestPort())
        .build();
  }

  private static HttpServer startServer() {
    config = ConfigDescriptor.getInstance().getConfig();
    baseURI = getbaseuri();
    final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iginx.rest");
    return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
  }

  public static void start() {
    HttpServer server = null;
    try {
      server = startServer();
    } catch (Exception e) {
      LOGGER.error("启动Rest服务({})失败，请检查是否启动了IoTDB服务以及相关配置参数是否正确", baseURI, e);
      System.exit(1);
    }
    LOGGER.info("Iginx REST server has been available at {}.", baseURI);
    try {
      Thread.currentThread().join();
    } catch (InterruptedException e) {
      LOGGER.error("Rest主线程出现异常", e);
      Thread.currentThread().interrupt();
    }
    server.shutdown();
  }

  public static void main(String[] argv) {
    start();
  }

  @Override
  public void run() {
    start();
  }
}
