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
import java.util.*;
import java.util.concurrent.*;
import org.junit.*;

public class MqttSecurityAcceptanceIT {
  static SecurityTestEnvironment env;
  static SecurityTestEnvironment.Node node;
  static Connection service;

  @BeforeClass
  public static void start() throws Exception {
    env = new SecurityTestEnvironment();
    node = env.node(0, secure("creator", "required"));
    node.config.setProperty("enableMqtt", "true");
    node.start();
    node.user("reader", AuthType.Read);
    node.user("writer", AuthType.Write);
    node.user("indagent_data_t1", AuthType.Read, AuthType.Write);
    service = node.connect("indagent_data_t1", "password");
  }

  @AfterClass
  public static void stop() throws Exception {
    if (env != null) env.close();
  }

  static long points(String path) throws Exception {
    QueryDataResp r =
        service.client.queryData(
            new QueryDataReq(service.id, Arrays.asList(path), 0, Long.MAX_VALUE));
    ok(r.status);
    return r.queryDataSet == null ? 0 : r.queryDataSet.getValuesListSize();
  }

  static void awaitPoints(String path, int count) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (points(path) < count && System.nanoTime() < end) Thread.sleep(40);
    assertEquals(count, points(path));
  }

  @Test
  public void igt28_authPublishSubscribeAndDisconnect() throws Exception {
    int sessions =
        node.admin.client.showSessionID(new ShowSessionIDReq(node.admin.id)).getSessionIDListSize();
    for (String[] credentials :
        new String[][] {{null, null}, {"reader", "wrong"}, {"missing", "password"}})
      try (SecurityMqttClient c =
          new SecurityMqttClient(node.mqtt, credentials[0], credentials[1])) {
        assertNotEquals(0, c.code);
      }
    for (String user : Arrays.asList("reader", "writer", "indagent_data_t1"))
      try (SecurityMqttClient c = new SecurityMqttClient(node.mqtt, user, "password")) {
        assertEquals(0, c.code);
        c.publish("mqtt_" + user + ".a", 1);
        c.publish("indagent.mqtt_" + user, 1);
        if (!user.equals("reader")) awaitPoints("mqtt_" + user + ".a", 1);
        if (user.equals("indagent_data_t1")) awaitPoints("indagent.mqtt_" + user, 1);
        else assertEquals(0, points("indagent.mqtt_" + user));
        if (user.equals("reader")) assertEquals(0, points("mqtt_" + user + ".a"));
        for (String topic : Arrays.asList("#", "events")) assertEquals(128, c.subscribe(topic));
        c.noMessage();
        if (user.equals("reader")) c.disconnect();
      }
    try (SecurityMqttClient root = new SecurityMqttClient(node.mqtt, "root", "root");
        SecurityMqttClient pub = new SecurityMqttClient(node.mqtt, "writer", "password")) {
      assertEquals(128, root.subscribe("#"));
      assertEquals(128, root.subscribe("events"));
      pub.publish("mqtt_delivery.a", 1);
      awaitPoints("mqtt_delivery.a", 1);
      root.noMessage();
    }
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (node.admin
                .client
                .showSessionID(new ShowSessionIDReq(node.admin.id))
                .getSessionIDListSize()
            != sessions
        && System.nanoTime() < deadline) Thread.sleep(20);
    assertEquals(
        sessions,
        node.admin
            .client
            .showSessionID(new ShowSessionIDReq(node.admin.id))
            .getSessionIDListSize());
    assertTrue(node.logs().contains("MQTT") && node.logs().contains("rejected"));
  }

  @Test
  public void igt29_continuousPublishPasswordGrantAndDrop() throws Exception {
    node.user("mqtt_revoke", AuthType.Read, AuthType.Write);
    String password = "password";
    for (int change = 0; change < 3; change++) {
      if (change == 2)
        ok(
            node.admin.client.updateUser(
                new UpdateUserReq()
                    .setSessionId(node.admin.id)
                    .setUsername("mqtt_revoke")
                    .setAuths(EnumSet.of(AuthType.Read, AuthType.Write))));
      String path = "mqtt_revoke.change" + change;
      try (SecurityMqttClient c = new SecurityMqttClient(node.mqtt, "mqtt_revoke", password)) {
        assertEquals(0, c.code);
        c.publish(path, 1);
        awaitPoints(path, 1);
        long changed = System.nanoTime();
        if (change == 0) {
          password = "changed";
          ok(
              node.admin.client.updateUser(
                  new UpdateUserReq()
                      .setSessionId(node.admin.id)
                      .setUsername("mqtt_revoke")
                      .setPassword(password)));
        } else if (change == 1)
          ok(
              node.admin.client.updateUser(
                  new UpdateUserReq()
                      .setSessionId(node.admin.id)
                      .setUsername("mqtt_revoke")
                      .setAuths(EnumSet.of(AuthType.Read))));
        else ok(node.admin.client.deleteUser(new DeleteUserReq(node.admin.id, "mqtt_revoke")));
        long deadline = changed + TimeUnit.SECONDS.toNanos(5);
        long key = 10;
        while (System.nanoTime() < deadline) {
          c.publish(path, key++);
          Thread.sleep(100);
        }
        long after = points(path);
        c.publish(path, 99999);
        Thread.sleep(150);
        assertEquals(after, points(path));
        assertEquals(1, after);
      }
      try (SecurityMqttClient fresh = new SecurityMqttClient(node.mqtt, "mqtt_revoke", password)) {
        if (change == 2) assertNotEquals(0, fresh.code);
        else {
          assertEquals(0, fresh.code);
          fresh.publish(path + "_new", 1);
          if (change == 0) awaitPoints(path + "_new", 1);
          else {
            Thread.sleep(200);
            assertEquals(0, points(path + "_new"));
          }
        }
      }
    }
  }
}
