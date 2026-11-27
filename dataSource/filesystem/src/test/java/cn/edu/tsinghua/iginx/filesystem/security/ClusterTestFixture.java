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

import cn.edu.tsinghua.iginx.filesystem.service.rpc.InternalAuth;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.thrift.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

final class ClusterTestFixture {
  static void start(SecurityTestEnvironment env, int replicas, String security, int engines)
      throws Exception {
    // Migration fixtures preserve existing replica metadata; first-start behavior is tested by PR
    // 8.
    if (replicas > 0) {
      org.apache.curator.framework.CuratorFramework client =
          org.apache.curator.framework.CuratorFrameworkFactory.newClient(
              "127.0.0.1:" + env.factory.getLocalPort(),
              new org.apache.curator.retry.RetryOneTime(100));
      client.start();
      try {
        client.blockUntilConnected(10, TimeUnit.SECONDS);
        client
            .create()
            .forPath(
                "/replicaNum",
                Integer.toString(replicas).getBytes(java.nio.charset.StandardCharsets.UTF_8));
      } finally {
        client.close();
      }
    }
    for (int i = 0; i < 3; i++) {
      SecurityTestEnvironment.Node n = env.node(replicas, security);
      if (i >= engines)
        n.config.setProperty(
            "storageEngineList", env.nodes.get(0).config.getProperty("storageEngineList"));
      n.start();
    }
    waitForCluster(env.nodes.get(0), 3, engines);
  }

  static void waitForCluster(SecurityTestEnvironment.Node node, int nodes, int engines)
      throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
    GetClusterInfoResp r;
    do {
      r = node.admin.client.getClusterInfo(new GetClusterInfoReq(node.admin.id));
      ok(r.status);
      if (liveEndpoints(r) == nodes && r.storageEngineInfos.size() == engines) return;
      Thread.sleep(100);
    } while (System.nanoTime() < end);
    assertEquals(nodes, liveEndpoints(r));
    assertEquals(engines, r.storageEngineInfos.size());
  }

  private static long liveEndpoints(GetClusterInfoResp cluster) {
    // etcd retains historical node identities across restarts. Count reachable endpoints,
    // not those persistent records; ZooKeeper uses ephemeral identities.
    return cluster.iginxInfos.stream()
        .filter(n -> !"false".equals(n.connectable))
        .map(n -> n.ip + ":" + n.port)
        .distinct()
        .count();
  }

  static void roundTrip(SecurityTestEnvironment env, int value) throws Exception {
    SecurityTestEnvironment.Node writer = env.nodes.get(value % env.nodes.size());
    long ready = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    ExecuteSqlResp written;
    do {
      written =
          writer.admin.sql("INSERT INTO rolling (key,a) VALUES (" + value + "," + value + ");");
      if (written.status.code == 200) break;
      Thread.sleep(100);
    } while (System.nanoTime() < ready);
    ok(written.status);
    for (SecurityTestEnvironment.Node reader : env.nodes) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      ExecuteSqlResp r;
      do {
        r = reader.admin.sql("SELECT a FROM rolling WHERE key=" + value + ";");
        if (r.status.code == 200 && r.queryDataSet != null && r.queryDataSet.valuesList.size() == 1)
          break;
        Thread.sleep(100);
      } while (System.nanoTime() < deadline);
      ok(r.status);
      assertEquals(1, r.paths.size());
      assertEquals(1, r.queryDataSet.valuesList.size());
      assertEquals(
          (long) value,
          ((Number)
                  cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesFromBufferAndBitmaps(
                          r.dataTypeList, r.queryDataSet.valuesList, r.queryDataSet.bitmapList)
                      .get(0)
                      .get(0))
              .longValue());
    }
  }

  static ExecuteSqlResp queryEventually(Connection connection, String sql) throws Exception {
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    ExecuteSqlResp result;
    do {
      result = connection.sql(sql);
      if (result.status.code == 200) return result;
      Thread.sleep(100);
    } while (System.nanoTime() < end);
    ok(result.status);
    return result;
  }

  static Map<DataUnit, RawDataSet> physical(SecurityTestEnvironment.Node node, String secret)
      throws Exception {
    try (TSocket socket = new TSocket("127.0.0.1", node.filesystem, 5000)) {
      socket.open();
      FileSystemRpc.Client client = new FileSystemRpc.Client(new TBinaryProtocol(socket));
      RawPrefix prefix = new RawPrefix().setPrefix("");
      prefix.setCredential(InternalAuth.sign(secret, "getUnits", prefix));
      Map<DataUnit, RawDataSet> result = new HashMap<>();
      for (DataUnit unit : client.getUnits(prefix).keySet()) {
        if (unit.dummy) continue;
        RawDataTarget target = new RawDataTarget().setPatterns(Arrays.asList("*"));
        RawAggregate aggregate = new RawAggregate();
        target.setCredential(InternalAuth.sign(secret, "query", unit, target, aggregate));
        result.put(unit, client.query(unit, target, aggregate));
      }
      return result;
    }
  }

  static void rejectRaw(SecurityTestEnvironment.Node node) throws Exception {
    Map<DataUnit, RawDataSet> before = physical(node, SECRET);
    try (TSocket socket = new TSocket("127.0.0.1", node.filesystem, 5000)) {
      socket.open();
      FileSystemRpc.Client client = new FileSystemRpc.Client(new TBinaryProtocol(socket));
      for (String credential : Arrays.asList(null, "incorrect-credential"))
        for (int method = 0; method < 4; method++) {
          DataUnit unit =
              before.isEmpty()
                  ? new DataUnit(false).setName("test")
                  : before.keySet().iterator().next();
          RawPrefix prefix = new RawPrefix().setPrefix("").setCredential(credential);
          RawDataTarget target =
              new RawDataTarget().setPatterns(Arrays.asList("*")).setCredential(credential);
          RawInserted inserted =
              new RawInserted()
                  .setPatterns(Collections.emptyList())
                  .setTagsList(Collections.emptyList())
                  .setKeys(new byte[0])
                  .setValuesList(Collections.emptyList())
                  .setBitmapList(Collections.emptyList())
                  .setDataTypeList(Collections.emptyList())
                  .setRawDataType("Row")
                  .setCredential(credential);
          try {
            switch (method) {
              case 0:
                client.getUnits(prefix);
                break;
              case 1:
                client.query(unit, target, new RawAggregate());
                break;
              case 2:
                client.insert(unit, inserted);
                break;
              default:
                client.delete(unit, target);
            }
            fail("Unauthenticated FileSystem method " + method + " accepted");
          } catch (RpcException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("authentication"));
          }
          assertEquals(before, physical(node, SECRET));
        }
    }
  }
}
