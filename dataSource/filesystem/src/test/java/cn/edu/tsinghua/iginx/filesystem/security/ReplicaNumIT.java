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
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;

public class ReplicaNumIT {
  static void shortage(SecurityTestEnvironment.Node node, CuratorFramework metadata)
      throws Exception {
    ExecuteSqlResp r = node.admin.sql("INSERT INTO replica (key,a) VALUES (1,1);");
    assertNotEquals(200, r.status.code);
    assertTrue(
        r.status.toString(),
        r.status.message.contains("not enough storage engines for replication"));
    assertFalse(r.status.toString().contains("IndexOutOfBounds"));
    for (String path : Arrays.asList("/fragment", "/unit"))
      assertTrue(
          path,
          metadata.checkExists().forPath(path) == null
              || metadata.getChildren().forPath(path).isEmpty());
  }

  static void copies(SecurityTestEnvironment.Node node, int number) throws Exception {
    GetReplicaNumResp r = node.admin.client.getReplicaNum(new GetReplicaNumReq(node.admin.id));
    ok(r.status);
    assertEquals(number, r.replicaNum);
    assertEquals(number, node.admin.execute("SHOW REPLICA NUMBER;").replicaNum);
  }

  static String batch(String prefix, int rows) {
    StringBuilder sql = new StringBuilder("INSERT INTO " + prefix + " (key,a) VALUES ");
    for (int i = 0; i < rows; i++) {
      if (i > 0) sql.append(',');
      sql.append('(').append(i).append(',').append(i + 100).append(')');
    }
    return sql.append(';').toString();
  }

  static SecurityTestEnvironment.Node primary(
      SecurityTestEnvironment env, CuratorFramework metadata) throws Exception {
    long engine = -1;
    for (String unit : metadata.getChildren().forPath("/unit")) {
      cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta value =
          cn.edu.tsinghua.iginx.utils.JsonUtils.fromJson(
              metadata.getData().forPath("/unit/" + unit),
              cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta.class);
      if (value != null && value.isMaster() && !value.isDummy()) {
        engine = value.getStorageEngineId();
        break;
      }
    }
    assertTrue("No primary storage unit", engine >= 0);
    for (StorageEngineInfo info :
        env.nodes
            .get(0)
            .admin
            .client
            .getClusterInfo(new GetClusterInfoReq(env.nodes.get(0).admin.id))
            .storageEngineInfos)
      if (info.id == engine)
        for (SecurityTestEnvironment.Node node : env.nodes)
          if (node.filesystem == info.port) return node;
    throw new AssertionError("Primary engine not in fixture: " + engine);
  }

  static ExecuteSqlResp readAfterFailure(Connection reader, String sql) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
    ExecuteSqlResp r;
    do {
      r = reader.sql(sql);
      if (r.status.code == 200) return r;
      Thread.sleep(100);
    } while (System.nanoTime() < end);
    ok(r.status);
    return r;
  }

  @Test(timeout = 150000)
  public void igt36_firstStartPreservesConfiguredReplicasAndSurvivesPrimaryLoss() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework metadata = ZkSecurityAcceptanceIT.client(env, false)) {
      SecurityTestEnvironment.Node first = env.node(1, null);
      first.start();
      copies(first, 2);
      assertEquals(
          "1", new String(metadata.getData().forPath("/replicaNum"), StandardCharsets.UTF_8));
      assertTrue(
          first.logs(),
          first.logs().contains("writable storage engines")
              && first.logs().contains("required for replication"));
      shortage(first, metadata);
      for (int i = 1; i < 3; i++) {
        SecurityTestEnvironment.Node node = env.node(1, null);
        node.start();
      }
      ClusterTestFixture.waitForCluster(first, 3, 3);
      for (SecurityTestEnvironment.Node node : env.nodes) copies(node, 2);
      first.admin.execute(batch("replica", 100));
      String before = first.admin.execute("SELECT * FROM replica;").toString();
      SecurityTestEnvironment.Node primary = primary(env, metadata);
      SecurityTestEnvironment.Node survivor =
          env.nodes.stream().filter(n -> n != primary).findFirst().get();
      // Wait for asynchronous physical replicas, then stop the actual primary rather than an
      // arbitrary IGinX process which may not own this fragment.
      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      int populated;
      do {
        populated = 0;
        for (SecurityTestEnvironment.Node node : env.nodes)
          if (ClusterTestFixture.physical(node, null).values().stream()
              .anyMatch(v -> v.rows.size() == 100)) populated++;
        if (populated >= 2) break;
        Thread.sleep(50);
      } while (System.nanoTime() < end);
      assertEquals(2, populated);
      primary.stop();
      assertEquals(before, readAfterFailure(survivor.admin, "SELECT * FROM replica;").toString());
    }
  }

  @Test(timeout = 90000)
  public void igt36_wildcardUsesReplicaAndFailsWhenEveryCopyIsOffline() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework metadata = ZkSecurityAcceptanceIT.client(env, true)) {
      ClusterTestFixture.start(env, 1, secure("creator", "required"), 2);
      Connection reader = env.nodes.get(2).admin;
      reader.execute(batch("schema_failover", 10));
      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      for (int i = 0; i < 2; i++) {
        while (ClusterTestFixture.physical(env.nodes.get(i), SECRET).values().stream()
                .noneMatch(data -> data.rows.size() == 10)
            && System.nanoTime() < end) Thread.sleep(50);
        assertTrue(
            ClusterTestFixture.physical(env.nodes.get(i), SECRET).values().stream()
                .anyMatch(data -> data.rows.size() == 10));
      }
      String before = reader.execute("SELECT * FROM *;").toString();
      SecurityTestEnvironment.Node primary = primary(env, metadata);
      primary.stop();
      assertEquals(before, readAfterFailure(reader, "SELECT * FROM *;").toString());
      for (int i = 0; i < 2; i++) if (env.nodes.get(i) != primary) env.nodes.get(i).stop();
      assertNotEquals(
          "Unavailable replicas must not look like an empty schema",
          200,
          reader.sql("SELECT * FROM *;").status.code);
    }
  }

  @Test(timeout = 90000)
  public void igt37_existingMetadataWinsOverConfiguration() throws Exception {
    try (SecurityTestEnvironment env = new SecurityTestEnvironment();
        CuratorFramework metadata = ZkSecurityAcceptanceIT.client(env, false)) {
      metadata.create().forPath("/replicaNum", "0".getBytes(StandardCharsets.UTF_8));
      ClusterTestFixture.start(env, 1, null, 3);
      for (SecurityTestEnvironment.Node node : env.nodes) copies(node, 1);
      env.nodes.get(0).admin.execute(batch("existing", 50));
      assertEquals(
          50,
          env.nodes.get(2).admin.execute("SELECT * FROM existing;").queryDataSet.valuesList.size());
    }
  }

  @Test(timeout = 150000)
  public void igt37_twoReplicasNeedThreeEnginesIncludingClientPartitioning() throws Exception {
    for (boolean clients : Arrays.asList(false, true))
      try (SecurityTestEnvironment env = new SecurityTestEnvironment();
          CuratorFramework metadata = ZkSecurityAcceptanceIT.client(env, false)) {
        for (int i = 0; i < 2; i++) {
          SecurityTestEnvironment.Node node = env.node(2, null);
          if (clients) {
            node.config.setProperty("clients", "client");
            node.config.setProperty("instancesNumPerClient", "1");
          }
          node.start();
        }
        shortage(env.nodes.get(0), metadata);
        SecurityTestEnvironment.Node last = env.node(2, null);
        if (clients) {
          last.config.setProperty("clients", "client");
          last.config.setProperty("instancesNumPerClient", "1");
        }
        last.start();
        for (SecurityTestEnvironment.Node node : env.nodes) copies(node, 3);
        env.nodes.get(0).admin.execute(batch(clients ? "client.instance0" : "replica", 50));
        assertEquals(50, last.admin.execute("SELECT * FROM *;").queryDataSet.valuesList.size());
      }
  }
}
