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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

/** Only selected by run_security_baseline.py, against freshly compiled pre-patch server classes. */
public class BaselineNegativeControlsIT {
  @Test(timeout = 90000)
  public void baselineProtocolCounterexamples() throws Exception {
    assertNotNull(
        "The baseline runner must supply original server classes",
        System.getProperty("iginx.acceptance.baselineRoot"));
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(0, secure("creator", "required"));
      node.config.setProperty("enableRestService", "true");
      node.config.setProperty("enableMqtt", "true");
      node.start();
      node.user("reader", AuthType.Read);
      node.user("writer", AuthType.Write);
      Connection reader = node.connect("reader", "password"),
          writer = node.connect("writer", "password");
      List<String> evidence = new ArrayList<>();
      for (long id : new long[] {0}) {
        try (Connection raw = node.raw()) {
          ok(
              raw.client.executeSql(
                      new ExecuteSqlReq(id, "INSERT INTO indagent.t (key,a) VALUES (1,1);"))
                  .status);
          ok(raw.client.executeSql(new ExecuteSqlReq(id, "SELECT * FROM indagent.t;")).status);
        }
        evidence.add("IGT-10/17 baseline accepts SQL with unopened session " + id);
      }
      try (Connection raw = node.raw()) {
        raw.socket.setTimeout(1500);
        try {
          Status response =
              raw.client.executeSql(
                      new ExecuteSqlReq(0x123456780000000fL, "SELECT * FROM indagent.t;"))
                  .status;
          assertNotEquals(101, response.code);
          evidence.add(
              "IGT-10 baseline random session was not rejected at entry: " + response.code);
        } catch (org.apache.thrift.transport.TTransportException expected) {
          assertTrue(
              expected.toString(), expected.getCause() instanceof java.net.SocketTimeoutException);
          evidence.add("IGT-10 baseline random session hangs instead of returning ACCESS_DENY");
        }
      }
      reader.execute("INSERT INTO ordinary (key,a) VALUES (1,11),(2,22),(3,33);");
      reader.execute("SHOW CONFIG;");
      reader.execute("SHOW SESSIONID;");
      writer.execute("SELECT * FROM ordinary;");
      evidence.add("IGT-11/14/15 baseline SQL bypasses Read/Write/Admin permissions");
      ExecuteStatementResp cursor =
          reader.client.executeStatement(
              new ExecuteStatementReq(reader.id, "SELECT * FROM ordinary;").setFetchSize(1));
      ok(cursor.status);
      ok(
          writer.client.fetchResults(new FetchResultsReq(writer.id, cursor.queryId).setFetchSize(1))
              .status);
      ok(writer.client.closeStatement(new CloseStatementReq(writer.id, cursor.queryId)));
      evidence.add("IGT-13 baseline permits foreign cursor fetch/close");
      ok(
          node.admin.client.updateUser(
              new UpdateUserReq(node.admin.id, "reader").setPassword("changed")));
      reader.execute("SELECT * FROM ordinary;");
      evidence.add("IGT-05/30 baseline old session survives password change");
      assertTrue(reader.execute("SHOW COLUMNS;").paths.contains("indagent.t.a"));
      reader.execute("DELETE COLUMNS indagent.t.a;");
      evidence.add("IGT-18/19 baseline exposes and deletes reserved columns");
      assertEquals(
          200,
          RestSecurityAcceptanceIT.http(
                  node, "POST", "/api/v1/datapoints", null, RestSecurityAcceptanceIT.INSERT)
              .code);
      assertEquals(
          200,
          RestSecurityAcceptanceIT.http(
                  node, "GET", "/", RestSecurityAcceptanceIT.auth("missing", "wrong"), null)
              .code);
      evidence.add("IGT-23/25 baseline REST accepts absent/wrong credentials");
      try (SecurityMqttClient mqtt = new SecurityMqttClient(node.mqtt, "missing", "wrong")) {
        assertEquals(0, mqtt.code);
        assertEquals(0, mqtt.subscribe("events"));
        mqtt.publish("baseline.mqtt", 1);
        mqtt.receive();
      }
      evidence.add(
          "IGT-28/63 baseline MQTT accepts arbitrary credentials and delivers subscriptions");
      assertFalse(ClusterTestFixture.physical(node, null).isEmpty());
      evidence.add("IGT-34 baseline unsigned internal getUnits/query accepted");
      try (CuratorFramework anonymous = ZkSecurityAcceptanceIT.client(env, false)) {
        String user =
            new String(anonymous.getData().forPath("/user/writer"), StandardCharsets.UTF_8);
        assertTrue(user, user.contains("password"));
        anonymous.create().forPath("/baseline-unprivileged");
        anonymous.delete().forPath("/baseline-unprivileged");
        evidence.add("IGT-40/41 baseline anonymous metadata read and root create/delete accepted");
      }
      Files.write(
          env.directory.resolve("baseline-counterexamples.txt"), evidence, StandardCharsets.UTF_8);
      for (String line : evidence) System.out.println(line);
    }
  }

  @Test
  public void baselineFirstStartSilentlyReducesReplicas() throws Exception {
    assertNotNull(System.getProperty("iginx.acceptance.baselineRoot"));
    try (SecurityTestEnvironment env = new SecurityTestEnvironment()) {
      SecurityTestEnvironment.Node node = env.node(1, null);
      node.start();
      assertEquals(1, node.admin.execute("SHOW REPLICA NUMBER;").replicaNum);
      node.admin.execute("INSERT INTO unreplicated (key,a) VALUES (1,1);");
      System.out.println(
          "IGT-36 baseline replicaNum=1 with one engine reports one total copy and accepts the write");
    }
  }
}
