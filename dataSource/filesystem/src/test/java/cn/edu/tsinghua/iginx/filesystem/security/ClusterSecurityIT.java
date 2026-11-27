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

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.filesystem.service.rpc.InternalAuth;
import cn.edu.tsinghua.iginx.filesystem.thrift.*;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.zookeeper.server.*;
import org.junit.Test;

/** Three independent JVMs exercise real metadata watches, RPCs and asynchronous replicas. */
public class ClusterSecurityIT {
  private static final String SECRET = "cluster-it-only-secret-1234567890";

  private static int port() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void ok(Status status) {
    assertEquals(status.toString(), RpcUtils.SUCCESS.code, status.code);
  }

  @Test(timeout = 180000)
  public void replicasAndRevocationAcrossThreeNodes() throws Exception {
    Path home = Files.createTempDirectory("iginx-security-cluster-");
    ZooKeeperServer zk =
        new ZooKeeperServer(home.resolve("snap").toFile(), home.resolve("log").toFile(), 2000);
    ServerCnxnFactory factory =
        ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
    factory.startup(zk);
    List<Node> nodes = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) {
        Node node = new Node(home.resolve("node" + i), factory.getLocalPort());
        nodes.add(node);
        node.start();
        ok(node.client.getReplicaNum(new GetReplicaNumReq(node.root)).getStatus());
        assertEquals(2, node.client.getReplicaNum(new GetReplicaNumReq(node.root)).getReplicaNum());
        if (i == 0) {
          ExecuteSqlResp response =
              node.client.executeSql(
                  new ExecuteSqlReq(node.root, "INSERT INTO early (key, a) VALUES (1, 1);"));
          assertNotEquals(RpcUtils.SUCCESS.code, response.getStatus().code);
          assertTrue(
              response.getStatus().toString(),
              response
                  .getStatus()
                  .getMessage()
                  .contains("not enough storage engines for replication"));
        }
      }
      Node first = nodes.get(0);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      GetClusterInfoResp cluster;
      do {
        cluster = first.client.getClusterInfo(new GetClusterInfoReq(first.root));
        if (cluster.getIginxInfosSize() == 3 && cluster.getStorageEngineInfosSize() == 3) break;
        Thread.sleep(50);
      } while (System.nanoTime() < deadline);
      assertEquals(3, cluster.getIginxInfosSize());
      assertEquals(3, cluster.getStorageEngineInfosSize());
      ok(
          first.client.addUser(
              new AddUserReq()
                  .setSessionId(first.root)
                  .setUsername("cluster_reader")
                  .setPassword("password")
                  .setAuths(EnumSet.of(AuthType.Read))));
      long[] sessions = new long[3];
      for (int i = 0; i < 3; i++) sessions[i] = nodes.get(i).login("cluster_reader", "password");
      ok(
          first.client.updateUser(
              new UpdateUserReq()
                  .setSessionId(first.root)
                  .setUsername("cluster_reader")
                  .setPassword("changed")));
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      boolean revoked = false;
      do {
        revoked = true;
        for (int i = 0; i < 3; i++)
          if (nodes.get(i).client.getReplicaNum(new GetReplicaNumReq(sessions[i])).getStatus().code
              != RpcUtils.ACCESS_DENY.code) revoked = false;
        if (revoked) break;
        Thread.sleep(20);
      } while (System.nanoTime() < deadline);
      assertTrue("All nodes must revoke within five seconds", revoked);
      for (Node node : nodes) {
        ok(node.client.getReplicaNum(new GetReplicaNumReq(node.root)).getStatus());
        node.login("cluster_reader", "changed");
      }
      ok(
          first.client.addUser(
              new AddUserReq()
                  .setSessionId(first.root)
                  .setUsername("indagent_data_cluster")
                  .setPassword("password")
                  .setAuths(EnumSet.of(AuthType.Read, AuthType.Write))));
      long service = first.login("indagent_data_cluster", "password");
      ok(
          first
              .client
              .executeSql(
                  new ExecuteSqlReq(
                      service, "INSERT INTO indagent.cluster (key, value) VALUES (1, 42);"))
              .getStatus());
      ok(first.client.closeSession(new CloseSessionReq(service)));
      // Read the physical replicas directly after closing the originating session.
      deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      int copies;
      do {
        copies = 0;
        for (Node node : nodes) if (node.hasPhysicalData()) copies++;
        if (copies >= 2) break;
        Thread.sleep(30);
      } while (System.nanoTime() < deadline);
      assertTrue("Accepted write must reach two independent engines", copies >= 2);
      first.stop();
      first.config.setProperty("replicaNum", "0");
      first.writeConfig();
      first.start();
      assertEquals(
          "Restart must retain persisted replica count",
          2,
          first.client.getReplicaNum(new GetReplicaNumReq(first.root)).getReplicaNum());
    } finally {
      for (Node node : nodes) node.stop();
      factory.shutdown();
      zk.shutdown();
    }
  }

  private static final class Node {
    final Path directory;
    final int rpc = port(), filesystem = port();
    final Properties config = new Properties();
    Process process;
    TSocket socket;
    IService.Client client;
    long root;

    Node(Path directory, int zkPort) throws Exception {
      this.directory = directory;
      Files.createDirectories(directory.resolve("driver/filesystem"));
      config.setProperty("ip", "127.0.0.1");
      config.setProperty("port", String.valueOf(rpc));
      config.setProperty("zookeeperConnectionString", "127.0.0.1:" + zkPort);
      config.setProperty(
          "storageEngineList",
          "127.0.0.1#"
              + filesystem
              + "#filesystem#iginx_port="
              + rpc
              + "#dir="
              + directory.resolve("data").toString().replace('\\', '/'));
      config.setProperty("replicaNum", "1");
      config.setProperty("queryOptimizer", "rbo");
      config.setProperty("useStreamExecutor", "false");
      config.setProperty("enableMemoryControl", "false");
      config.setProperty("enableRestService", "false");
      config.setProperty("pythonCMD", "python");
      config.setProperty("memoryTaskThreadPoolSize", "4");
      config.setProperty("physicalTaskThreadPoolSizePerStorage", "4");
      config.setProperty("defaultUDFDir", directory.resolve("udf").toString());
      config.setProperty("defaultScheduledTransformJobDir", directory.resolve("jobs").toString());
      writeConfig();
      Path security = directory.resolve("security.properties");
      Files.write(
          security,
          ("zookeeper.user=meta\nzookeeper.password="
                  + SECRET
                  + "\nzookeeper.acl=creator\ninternal.secret="
                  + SECRET
                  + "\ninternal.auth=required\nreserved.prefixes=indagent\nreserved.accounts=indagent_data_*\n")
              .getBytes(StandardCharsets.UTF_8));
      if (Files.getFileStore(security).supportsFileAttributeView("posix"))
        Files.setPosixFilePermissions(
            security, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    }

    void writeConfig() throws IOException {
      try (OutputStream out = Files.newOutputStream(directory.resolve("config.properties"))) {
        config.store(out, "cluster test");
      }
    }

    void start() throws Exception {
      List<String> command = new ArrayList<>();
      command.add(
          Paths.get(
                  System.getProperty("java.home"),
                  "bin",
                  System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
              .toString());
      if (!System.getProperty("java.specification.version").equals("1.8"))
        command.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
      Collections.addAll(
          command,
          "-Dfile.encoding=UTF-8",
          "-DIGINX_HOME=" + directory,
          "-DIGINX_CONF=" + directory.resolve("config.properties"),
          "-DIGINX_DRIVER=" + directory.resolve("driver"),
          "-cp",
          System.getProperty("java.class.path"),
          "cn.edu.tsinghua.iginx.Iginx");
      process =
          new ProcessBuilder(command)
              .redirectErrorStream(true)
              .redirectOutput(
                  ProcessBuilder.Redirect.appendTo(directory.resolve("node.log").toFile()))
              .start();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (System.nanoTime() < deadline) {
        assertTrue("Node exited; log: " + directory.resolve("node.log"), process.isAlive());
        socket = new TSocket("127.0.0.1", rpc, 1000);
        try {
          socket.open();
          break;
        } catch (Exception e) {
          socket.close();
          Thread.sleep(100);
        }
      }
      assertTrue("Node not listening; log: " + directory.resolve("node.log"), socket.isOpen());
      socket.setTimeout(15000);
      client = new IService.Client(new TBinaryProtocol(socket));
      root = login("root", "root");
    }

    long login(String name, String password) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      OpenSessionResp response;
      do {
        response = client.openSession(new OpenSessionReq().setUsername(name).setPassword(password));
        if (response.getSessionId() != 0) {
          ok(response.getStatus());
          return response.getSessionId();
        }
        Thread.sleep(30);
      } while (System.nanoTime() < deadline);
      throw new AssertionError("Login did not propagate: " + name);
    }

    boolean hasPhysicalData() throws Exception {
      try (TSocket transport = new TSocket("127.0.0.1", filesystem, 5000)) {
        transport.open();
        FileSystemRpc.Client raw = new FileSystemRpc.Client(new TBinaryProtocol(transport));
        RawPrefix prefix = new RawPrefix().setPrefix("");
        prefix.setCredential(InternalAuth.sign(SECRET, "getUnits", prefix));
        for (DataUnit unit : raw.getUnits(prefix).keySet()) {
          RawDataTarget target =
              new RawDataTarget().setPatterns(Arrays.asList("indagent.cluster.value"));
          RawAggregate aggregate = new RawAggregate();
          target.setCredential(InternalAuth.sign(SECRET, "query", unit, target, aggregate));
          if (!raw.query(unit, target, aggregate).getRows().isEmpty()) return true;
        }
        return false;
      }
    }

    void stop() throws Exception {
      if (socket != null) socket.close();
      if (process != null) {
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly();
      }
    }
  }
}
