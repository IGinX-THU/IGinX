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
package cn.edu.tsinghua.iginx.filesystem.security;

import static cn.edu.tsinghua.iginx.filesystem.security.SecurityTestEnvironment.*;
import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.thrift.*;
import java.nio.file.Files;
import org.junit.Test;

/** IGT-24 and IGT-27: persistent identity, including the actual restart boundary. */
public class RootPasswordIT {
  private static final String CHANGED = "root-password-after-initialization";

  private static void rejected(SecurityTestEnvironment.Node node) throws Exception {
    try (Connection raw = node.raw()) {
      OpenSessionResp login =
          raw.client.openSession(new OpenSessionReq().setUsername("root").setPassword("root"));
      assertNotEquals(200, login.status.code);
      assertEquals(0, login.sessionId);
      denied(raw.client.executeSql(new ExecuteSqlReq(login.sessionId, "SELECT * FROM *;")).status);
    }
  }

  private static void gateways(SecurityTestEnvironment.Node node) throws Exception {
    checkGateway(node, "ordinary", "password", true);
    checkGateway(node, "root", "root", false);
    checkGateway(node, "root", CHANGED, true);
  }

  private static void checkGateway(
      SecurityTestEnvironment.Node node, String user, String password, boolean accepted)
      throws Exception {
    RestSecurityAcceptanceIT.Response response =
        RestSecurityAcceptanceIT.http(
            node,
            "POST",
            "/api/v1/datapoints",
            RestSecurityAcceptanceIT.auth(user, password),
            RestSecurityAcceptanceIT.INSERT);
    assertEquals(response.body, accepted ? 200 : 401, response.code);
    try (SecurityMqttClient mqtt = new SecurityMqttClient(node.mqtt, user, password)) {
      if (accepted) {
        assertEquals(0, mqtt.code);
        mqtt.publish("business.mqtt", 9);
      } else assertNotEquals(0, mqtt.code);
    }
    if (accepted) {
      long end = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (node.admin.execute("SELECT * FROM business;").paths.size() < 2
          && System.nanoTime() < end) Thread.sleep(50);
      assertEquals(2, node.admin.execute("SELECT * FROM business;").paths.size());
    }
  }

  @Test(timeout = 120000)
  public void igt24_27_changePasswordVerifyAfterRestartAndRestore() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.config.setProperty("enableRestService", "true");
      node.config.setProperty("enableMqtt", "true");
      node.start();
      node.user("ordinary", AuthType.Read, AuthType.Write);
      byte[] original = Files.readAllBytes(node.home.resolve("config.properties"));
      node.admin.execute("ALTER USER root IDENTIFIED BY `" + CHANGED + "`;");
      node.admin = node.connect("root", CHANGED);
      try {
        rejected(node);
        gateways(node);
        node.stop();
        node.start(CHANGED);
        // Properties.store changes its timestamp, so compare the actual values as well as keeping
        // the original file for the restart. Neither username nor password is updated.
        java.util.Properties before = new java.util.Properties();
        before.load(new java.io.ByteArrayInputStream(original));
        java.util.Properties after = new java.util.Properties();
        try (java.io.InputStream in =
            Files.newInputStream(node.home.resolve("config.properties"))) {
          after.load(in);
        }
        assertEquals(before, after);
        rejected(node);
        gateways(node);
      } finally {
        if (node.process != null && node.process.isAlive()) {
          node.admin = node.connect("root", CHANGED);
          node.admin.execute("ALTER USER root IDENTIFIED BY root;");
        }
      }
      node.stop();
      node.start();
      ok(node.admin.sql("SELECT * FROM business;").status);
    }
  }
}
