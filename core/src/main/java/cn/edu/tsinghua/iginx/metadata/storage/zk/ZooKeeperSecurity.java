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

import cn.edu.tsinghua.iginx.metadata.storage.constant.Constant;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

public final class ZooKeeperSecurity {
  private ZooKeeperSecurity() {}

  public static CuratorFrameworkFactory.Builder configure(
      CuratorFrameworkFactory.Builder builder, SecurityConfig security) {
    if (security.get("zookeeper.user") != null) {
      builder.authorization(
          "digest",
          (security.get("zookeeper.user") + ":" + security.get("zookeeper.password"))
              .getBytes(StandardCharsets.UTF_8));
      if ("creator".equals(security.get("zookeeper.acl"))) {
        builder.aclProvider(
            new ACLProvider() {
              public List<ACL> getDefaultAcl() {
                return ZooDefs.Ids.CREATOR_ALL_ACL;
              }

              public List<ACL> getAclForPath(String path) {
                return getDefaultAcl();
              }
            });
      }
    }
    return builder;
  }

  public static Set<String> metadataRoots() {
    Set<String> roots = new TreeSet<>();
    for (String value : paths()) roots.add("/" + value.substring(1).split("/")[0]);
    roots.add("/policy");
    return Collections.unmodifiableSet(roots);
  }

  public static Set<String> lockParents() {
    Set<String> paths =
        new TreeSet<>(
            Comparator.comparingInt(String::length).thenComparing(Comparator.naturalOrder()));
    List<String> locks = paths();
    locks.add("/lock/policy");
    for (String path : locks) {
      if (!path.startsWith("/lock/")) continue;
      while (!path.isEmpty()) {
        paths.add(path);
        path = path.substring(0, path.lastIndexOf('/'));
      }
    }
    return Collections.unmodifiableSet(paths);
  }

  private static List<String> paths() {
    List<String> paths = new ArrayList<>();
    for (Field field : Constant.class.getFields()) {
      if (field.getType() == String.class) {
        try {
          String value = (String) field.get(null);
          if (value.startsWith("/")) paths.add(value);
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return paths;
  }

  public static void ensurePersistentLocks(CuratorFramework client) {
    try {
      for (String path : lockParents()) {
        try {
          client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
          markPersistent(client, path, SecurityConfig.getInstance());
        } catch (KeeperException.NodeExistsException ignored) {
          /* Migration handles existing containers. */
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Cannot initialize persistent metadata locks", e);
    }
  }

  // ZooKeeper 3.7.2 masks container owners to zero in client Stat responses. Bind a
  // proof to the node's creation zxid, so an open-ACL client cannot forge or replay
  // a persistent-node marker onto a replacement container. Curator lock data is unused.
  static byte[] persistentProof(String path, long creationId, SecurityConfig security) {
    try {
      javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
      mac.init(
          new javax.crypto.spec.SecretKeySpec(
              security.get("zookeeper.password").getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      return mac.doFinal(
          ("iginx-persistent-lock-v1:" + path + ":" + creationId).getBytes(StandardCharsets.UTF_8));
    } catch (java.security.GeneralSecurityException e) {
      throw new IllegalStateException(e);
    }
  }

  static void markPersistent(CuratorFramework client, String path, SecurityConfig security)
      throws Exception {
    org.apache.zookeeper.data.Stat stat = client.checkExists().forPath(path);
    client
        .setData()
        .withVersion(stat.getVersion())
        .forPath(path, persistentProof(path, stat.getCzxid(), security));
  }

  static boolean verifiedPersistent(CuratorFramework client, String path, SecurityConfig security)
      throws Exception {
    org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
    byte[] data = client.getData().storingStatIn(stat).forPath(path);
    return stat.getEphemeralOwner() == 0
        && java.security.MessageDigest.isEqual(
            data, persistentProof(path, stat.getCzxid(), security));
  }
}
