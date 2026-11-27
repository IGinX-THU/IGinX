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
package cn.edu.tsinghua.iginx.metadata.storage.zk;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperMembershipTest {
  private ZooKeeperServer server;
  private ServerCnxnFactory factory;
  private final List<CuratorFramework> clients = new ArrayList<>();

  @Before
  public void start() throws Exception {
    Path directory = Files.createTempDirectory("iginx-membership-");
    server =
        new ZooKeeperServer(
            directory.resolve("snap").toFile(), directory.resolve("log").toFile(), 2000);
    factory = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
    factory.startup(server);
  }

  private CuratorFramework client() throws Exception {
    CuratorFramework client =
        CuratorFrameworkFactory.newClient(
            "127.0.0.1:" + factory.getLocalPort(), new ExponentialBackoffRetry(20, 2));
    clients.add(client);
    client.start();
    assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
    return client;
  }

  @After
  public void stop() {
    for (CuratorFramework client : clients) client.close();
    if (factory != null) factory.shutdown();
    if (server != null) server.shutdown();
  }

  @Test(timeout = 30000)
  public void departureBetweenListingAndReadingPreservesOtherMembers() throws Exception {
    CuratorFramework observer = client();
    CuratorFramework departing = client();
    departing
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(
            "/iginx/info/node0",
            JsonUtils.toJson(new IginxMeta(0, "127.0.0.1", 6888, Collections.emptyMap())));
    observer
        .create()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(
            "/iginx/info/node1",
            JsonUtils.toJson(new IginxMeta(1, "127.0.0.1", 6889, Collections.emptyMap())));
    List<String> snapshot = observer.getChildren().forPath("/iginx/info");
    assertEquals(2, snapshot.size());
    departing.close();
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (observer.checkExists().forPath("/iginx/info/node0") != null) {
      assertTrue("Ephemeral member did not depart", System.nanoTime() < end);
      Thread.sleep(10);
    }
    Map<Long, IginxMeta> members = ZooKeeperMetaStorage.readIginxMembers(observer, snapshot);
    assertEquals(Collections.singleton(1L), members.keySet());
    assertEquals(6889, members.get(1L).getPort());
  }

  @Test(timeout = 30000)
  public void permissionFailureIsNotTreatedAsDeparture() throws Exception {
    CuratorFramework owner = client();
    owner
        .getZookeeperClient()
        .getZooKeeper()
        .addAuthInfo("digest", "owner:secret".getBytes(StandardCharsets.UTF_8));
    owner.create().creatingParentsIfNeeded().forPath("/iginx/info");
    owner
        .create()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(ZooDefs.Ids.CREATOR_ALL_ACL)
        .forPath(
            "/iginx/info/node0",
            JsonUtils.toJson(new IginxMeta(0, "127.0.0.1", 6888, Collections.emptyMap())));
    CuratorFramework anonymous = client();
    try {
      ZooKeeperMetaStorage.readIginxMembers(
          anonymous, anonymous.getChildren().forPath("/iginx/info"));
      fail("An unreadable live member must fail closed");
    } catch (KeeperException.NoAuthException expected) {
      // Only NoNode from a departed member may be ignored.
    }
  }

  private void departedConnectionsAreIgnored(String parent) throws Exception {
    CuratorFramework observer = client();
    CuratorFramework departing = client();
    departing
        .create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(parent + "/node0", JsonUtils.toJson(new long[] {10}));
    observer
        .create()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(parent + "/node1", JsonUtils.toJson(new long[] {10, 11}));
    List<String> snapshot = observer.getChildren().forPath(parent);
    assertEquals(2, snapshot.size());
    departing.close();
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (observer.checkExists().forPath(parent + "/node0") != null) {
      assertTrue("Ephemeral connections did not depart", System.nanoTime() < end);
      Thread.sleep(10);
    }
    Map<Long, Set<Long>> connections =
        ZooKeeperMetaStorage.readConnections(observer, parent, snapshot);
    assertEquals(Collections.singleton(1L), connections.keySet());
    assertEquals(new HashSet<>(Arrays.asList(10L, 11L)), connections.get(1L));
  }

  @Test(timeout = 30000)
  public void storageConnectionsSurviveDepartureDuringSnapshot() throws Exception {
    departedConnectionsAreIgnored("/iginx/connection-storage");
  }

  @Test(timeout = 30000)
  public void peerConnectionsSurviveDepartureDuringSnapshot() throws Exception {
    departedConnectionsAreIgnored("/iginx/connection-iginx");
  }

  private void unreadableConnectionsFail(String parent) throws Exception {
    CuratorFramework owner = client();
    owner
        .getZookeeperClient()
        .getZooKeeper()
        .addAuthInfo("digest", "owner:secret".getBytes(StandardCharsets.UTF_8));
    owner.create().creatingParentsIfNeeded().forPath(parent);
    owner
        .create()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(ZooDefs.Ids.CREATOR_ALL_ACL)
        .forPath(parent + "/node0", JsonUtils.toJson(new long[] {10}));
    CuratorFramework anonymous = client();
    try {
      ZooKeeperMetaStorage.readConnections(
          anonymous, parent, anonymous.getChildren().forPath(parent));
      fail("An unreadable live connection record must fail closed");
    } catch (KeeperException.NoAuthException expected) {
      // Losing read permission must not turn live connections into an empty snapshot.
    }
  }

  @Test(timeout = 30000)
  public void storageConnectionPermissionFailurePropagates() throws Exception {
    unreadableConnectionsFail("/iginx/connection-storage");
  }

  @Test(timeout = 30000)
  public void peerConnectionPermissionFailurePropagates() throws Exception {
    unreadableConnectionsFail("/iginx/connection-iginx");
  }
}
