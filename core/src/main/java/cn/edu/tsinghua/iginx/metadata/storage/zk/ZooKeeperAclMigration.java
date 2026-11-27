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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

/** Offline/operator command. It never discovers application roots by traversing '/'. */
public final class ZooKeeperAclMigration {
  public static void main(String[] args) {
    int status = 1;
    try {
      if (args.length != 1 || !Arrays.asList("check", "apply", "revert").contains(args[0])) {
        throw new IllegalArgumentException("Usage: zk_acl_migrate check|apply|revert");
      }
      SecurityConfig security = SecurityConfig.getInstance();
      if (security.get("zookeeper.user") == null)
        throw new IllegalArgumentException("zookeeper.user is required");
      try (CuratorFramework client =
          ZooKeeperSecurity.configure(
                  CuratorFrameworkFactory.builder()
                      .connectString(
                          ConfigDescriptor.getInstance().getConfig().getZookeeperConnectionString())
                      .retryPolicy(new ExponentialBackoffRetry(500, 3))
                      .connectionTimeoutMs(10000),
                  security)
              .build()) {
        client.start();
        if (!client.blockUntilConnected(10, TimeUnit.SECONDS))
          throw new IllegalStateException("ZooKeeper connection timed out");
        status = run(client, security, args[0]);
      }
    } catch (Exception e) {
      // Do not print configuration or authentication material, including exception cause chains.
      if (e instanceof KeeperException.NoAuthException
          || e instanceof KeeperException.AuthFailedException) {
        System.err.println("Metadata ACL command failed: account or password is incorrect");
      } else if (e instanceof KeeperException.ConnectionLossException
          || e instanceof IllegalStateException) {
        System.err.println("Metadata ACL command failed: cannot connect to ZooKeeper");
      } else System.err.println("Metadata ACL command failed: " + e.getClass().getSimpleName());
    }
    System.exit(status);
  }

  public static int run(CuratorFramework client, SecurityConfig security, String action)
      throws Exception {
    if (!Arrays.asList("check", "apply", "revert").contains(action))
      throw new IllegalArgumentException("Unknown action");
    String digest =
        DigestAuthenticationProvider.generateDigest(
            security.get("zookeeper.user") + ":" + security.get("zookeeper.password"));
    List<ACL> owner =
        Collections.singletonList(new ACL(ZooDefs.Perms.ALL, new Id("digest", digest)));
    List<ACL> root =
        Arrays.asList(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE), owner.get(0));
    boolean check = action.equals("check");
    boolean revert = action.equals("revert");
    int issues = 0;
    int[] changed = {0};
    for (String path : ZooKeeperSecurity.metadataRoots()) {
      if (client.checkExists().forPath(path) != null)
        issues += visit(client, path, check, revert ? ZooDefs.Ids.OPEN_ACL_UNSAFE : owner, changed);
    }
    if (check) {
      if (!same(client.getACL().forPath("/"), root)) {
        System.err.println("Unexpected root ACL: /");
        issues++;
      }
    } else {
      List<ACL> expected = revert ? ZooDefs.Ids.OPEN_ACL_UNSAFE : root;
      if (!same(client.getACL().forPath("/"), expected)) {
        client.setACL().withACL(expected).forPath("/");
        changed[0]++;
      }
    }
    if (!revert) {
      // Children first: an empty container's ancestors may themselves become empty.
      List<String> locks = new ArrayList<>(ZooKeeperSecurity.lockParents());
      Collections.reverse(locks);
      for (String path : locks) {
        Stat stat = client.checkExists().forPath(path);
        if (stat != null && !ZooKeeperSecurity.verifiedPersistent(client, path, security)) {
          if (check) {
            System.err.println("Unverified persistent lock parent (may be a container): " + path);
            issues++;
          } else {
            if (!makePersistent(client, path, owner, security)) {
              System.err.println("Active lock parent skipped: " + path);
              issues++;
            } else changed[0]++;
          }
        }
      }
    }
    System.out.println("ACL " + action + ": " + changed[0] + " changed, " + issues + " unresolved");
    return issues == 0 ? 0 : 1;
  }

  private static int visit(
      CuratorFramework client, String path, boolean check, List<ACL> expected, int[] changed)
      throws Exception {
    int issues = 0;
    try {
      if (check) {
        if (!same(client.getACL().forPath(path), expected)) {
          System.err.println("Unexpected ACL: " + path);
          issues++;
        }
      } else if (!same(client.getACL().forPath(path), expected)) {
        client.setACL().withACL(expected).forPath(path);
        changed[0]++;
      }
      for (String child : client.getChildren().forPath(path))
        issues += visit(client, path + "/" + child, check, expected, changed);
    } catch (KeeperException.NoNodeException ignored) {
      /* Ephemeral node expired concurrently. */
    }
    return issues;
  }

  private static boolean same(List<ACL> actual, List<ACL> expected) {
    return new HashSet<>(actual).equals(new HashSet<>(expected));
  }

  private static boolean makePersistent(
      CuratorFramework client, String path, List<ACL> acl, SecurityConfig security)
      throws Exception {
    Map<String, byte[]> data = new LinkedHashMap<>();
    Map<String, Stat> stats = new HashMap<>();
    if (!snapshotLocks(client, path, data, stats)) return false;
    List<String> paths = new ArrayList<>(data.keySet());
    List<org.apache.curator.framework.api.transaction.CuratorOp> operations = new ArrayList<>();
    Collections.reverse(paths);
    for (String node : paths)
      operations.add(
          client.transactionOp().delete().withVersion(stats.get(node).getVersion()).forPath(node));
    Collections.reverse(paths);
    for (String node : paths)
      operations.add(
          client
              .transactionOp()
              .create()
              .withMode(CreateMode.PERSISTENT)
              .withACL(acl)
              .forPath(node, data.get(node)));
    try {
      // Atomic replacement leaves no interval in which another client can recreate the parent.
      client.transaction().forOperations(operations);
      for (String node : paths) ZooKeeperSecurity.markPersistent(client, node, security);
      return true;
    } catch (KeeperException.NotEmptyException
        | KeeperException.NodeExistsException
        | KeeperException.NoNodeException
        | KeeperException.BadVersionException e) {
      return false;
    }
  }

  private static boolean snapshotLocks(
      CuratorFramework client, String path, Map<String, byte[]> data, Map<String, Stat> stats)
      throws Exception {
    if (!ZooKeeperSecurity.lockParents().contains(path)) return false;
    Stat stat = new Stat();
    data.put(path, client.getData().storingStatIn(stat).forPath(path));
    stats.put(path, stat);
    for (String child : client.getChildren().forPath(path)) {
      if (!snapshotLocks(client, path + "/" + child, data, stats)) return false;
    }
    return true;
  }
}
