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
package cn.edu.tsinghua.iginx.auth;

import static org.junit.Assert.*;

import cn.edu.tsinghua.iginx.metadata.storage.zk.*;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.server.*;
import org.junit.Test;

public class ZkAclTest {
  private SecurityConfig security(Path home, String mode) throws Exception {
    Path file = home.resolve("security.properties");
    Files.write(
        file,
        ("zookeeper.user=meta\nzookeeper.password=123456789012345678901234\nzookeeper.acl=" + mode)
            .getBytes(StandardCharsets.UTF_8));
    if (Files.getFileStore(file).supportsFileAttributeView("posix"))
      Files.setPosixFilePermissions(
          file, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    return SecurityConfig.load(file);
  }

  @Test
  public void persistentLocksAndMigration() throws Exception {
    Path directory = Files.createTempDirectory("iginx-zk-security-");
    ZooKeeperServer server =
        new ZooKeeperServer(
            directory.resolve("snap").toFile(), directory.resolve("log").toFile(), 2000);
    ServerCnxnFactory factory =
        ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 100);
    factory.startup(server);
    String endpoint = "127.0.0.1:" + factory.getLocalPort();
    SecurityConfig security = security(directory, "open");
    try (CuratorFramework owner =
            ZooKeeperSecurity.configure(
                    CuratorFrameworkFactory.builder()
                        .connectString(endpoint)
                        .retryPolicy(new ExponentialBackoffRetry(50, 2)),
                    security)
                .build();
        CuratorFramework anonymous =
            CuratorFrameworkFactory.newClient(endpoint, new ExponentialBackoffRetry(50, 2))) {
      owner.start();
      anonymous.start();
      assertTrue(owner.blockUntilConnected(5, TimeUnit.SECONDS));
      assertTrue(anonymous.blockUntilConnected(5, TimeUnit.SECONDS));
      owner
          .create()
          .creatingParentsIfNeeded()
          .forPath("/user/u", "user-secret".getBytes(StandardCharsets.UTF_8));
      owner.create().forPath("/indagent", new byte[] {7});
      owner.create().forPath("/other", new byte[] {8});
      List<ACL> otherAcl = owner.getACL().forPath("/other");
      for (String path : ZooKeeperSecurity.lockParents())
        if (owner.checkExists().forPath(path) == null)
          owner
              .create()
              .creatingParentContainersIfNeeded()
              .withMode(CreateMode.CONTAINER)
              .forPath(path);
      assertEquals(1, ZooKeeperAclMigration.run(owner, security, "check"));
      owner.create().withMode(CreateMode.EPHEMERAL).forPath("/lock/user/held");
      assertEquals(1, ZooKeeperAclMigration.run(owner, security, "apply"));
      owner.delete().forPath("/lock/user/held");
      assertEquals(0, ZooKeeperAclMigration.run(owner, security, "apply"));
      assertEquals(0, ZooKeeperAclMigration.run(owner, security, "check"));
      assertEquals(0, ZooKeeperAclMigration.run(owner, security, "apply"));
      assertEquals(25, ZooKeeperSecurity.lockParents().size());
      for (String path : ZooKeeperSecurity.lockParents()) {
        assertEquals(path, 0, owner.checkExists().forPath(path).getEphemeralOwner());
        assertFalse(path, server.getZKDatabase().getDataTree().getContainers().contains(path));
      }
      for (String path : Arrays.asList("/user", "/user/u", "/lock", "/lock/user")) {
        try {
          anonymous.getData().forPath(path);
          fail("Unauthenticated read " + path);
        } catch (KeeperException.NoAuthException expected) {
        }
        try {
          anonymous.create().forPath(path + "/forbidden");
          fail("Unauthenticated write " + path);
        } catch (KeeperException.NoAuthException expected) {
        }
      }
      try {
        anonymous.create().forPath("/forbidden");
        fail("Root create permitted");
      } catch (KeeperException.NoAuthException expected) {
      }
      assertArrayEquals(new byte[] {7}, anonymous.getData().forPath("/indagent"));
      assertEquals(otherAcl, owner.getACL().forPath("/other"));
      assertEquals(0, ZooKeeperAclMigration.run(owner, security, "revert"));
      assertEquals(0, ZooKeeperAclMigration.run(owner, security, "revert"));
      assertEquals(
          "user-secret",
          new String(anonymous.getData().forPath("/user/u"), StandardCharsets.UTF_8));
      assertEquals(1, ZooKeeperAclMigration.run(owner, security, "check"));
    } finally {
      factory.shutdown();
      server.shutdown();
    }
  }

  @Test
  public void creatorAclProvider() throws Exception {
    Path directory = Files.createTempDirectory("iginx-zk-provider-");
    CuratorFrameworkFactory.Builder builder =
        ZooKeeperSecurity.configure(
            CuratorFrameworkFactory.builder(), security(directory, "creator"));
    assertEquals(ZooDefs.Ids.CREATOR_ALL_ACL, builder.getAclProvider().getDefaultAcl());
    assertEquals(1, builder.getAuthInfos().size());
    assertEquals("digest", builder.getAuthInfos().get(0).getScheme());
    assertArrayEquals(
        "meta:123456789012345678901234".getBytes(StandardCharsets.UTF_8),
        builder.getAuthInfos().get(0).getAuth());
    CuratorFrameworkFactory.Builder absent =
        ZooKeeperSecurity.configure(
            CuratorFrameworkFactory.builder(),
            SecurityConfig.load(directory.resolve("missing-security.properties")));
    assertTrue(absent.getAuthInfos() == null || absent.getAuthInfos().isEmpty());
    assertEquals(ZooDefs.Ids.OPEN_ACL_UNSAFE, absent.getAclProvider().getDefaultAcl());
    assertEquals(ZooDefs.Ids.CREATOR_ALL_ACL, builder.getAclProvider().getAclForPath("/user/new"));
    assertEquals(
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        ZooKeeperSecurity.configure(CuratorFrameworkFactory.builder(), security(directory, "open"))
            .getAclProvider()
            .getDefaultAcl());
  }
}
