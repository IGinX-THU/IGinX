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
package cn.edu.tsinghua.iginx.metadata.storage.etcd;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperSecurity;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import io.etcd.jetcd.*;
import io.etcd.jetcd.auth.Permission;
import io.etcd.jetcd.options.OptionsUtil;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Provisions a dedicated least-privilege etcd identity; never toggles cluster-wide authentication.
 */
public final class EtcdAuthMigration {
  private static Set<String> metadataRoots() {
    Set<String> roots = new TreeSet<>(ZooKeeperSecurity.metadataRoots());
    roots.add("/id");
    return roots;
  }

  private static ByteSequence bytes(String value) {
    return ByteSequence.from(value, StandardCharsets.UTF_8);
  }

  private static <T> T waitFor(CompletableFuture<T> future) throws Exception {
    return future.get(15, TimeUnit.SECONDS);
  }

  public static void main(String[] args) {
    int result = 1;
    try {
      if (args.length != 1 || !Arrays.asList("apply", "check", "revert").contains(args[0]))
        throw new IllegalArgumentException("Usage: etcd_auth_migrate check|apply|revert");
      SecurityConfig security = SecurityConfig.getInstance();
      String username = security.get("etcd.user");
      if (username == null || username.equals("root"))
        throw new IllegalArgumentException("A dedicated non-root etcd.user is required");
      String password = System.getenv("IGINX_ETCD_ROOT_PASSWORD");
      if (password == null && System.console() != null) {
        char[] input = System.console().readPassword("etcd root password: ");
        if (input != null) {
          password = new String(input);
          Arrays.fill(input, '\0');
        }
      }
      if (password == null || password.isEmpty())
        throw new IllegalArgumentException(
            "Set IGINX_ETCD_ROOT_PASSWORD or use an interactive console");
      String[] endpoints = ConfigDescriptor.getInstance().getConfig().getEtcdEndpoints().split(",");
      try (Client client = operatorClient(endpoints, password)) {
        result = run(client.getAuthClient(), security, args[0]);
      }
      if (args[0].equals("check") && result == 0) {
        try (Client client =
            Client.builder()
                .endpoints(endpoints)
                .user(bytes(username))
                .password(bytes(security.get("etcd.password")))
                .build()) {
          for (String root : metadataRoots()) waitFor(client.getKVClient().get(bytes(root)));
        }
        try (Client anonymous = Client.builder().endpoints(endpoints).build()) {
          try {
            waitFor(anonymous.getKVClient().get(bytes("/user")));
            System.err.println("etcd authentication is not enabled");
            result = 1;
          } catch (Exception e) {
            io.grpc.Status.Code code =
                io.grpc.Status.fromThrowable(e.getCause() == null ? e : e.getCause()).getCode();
            if (code != io.grpc.Status.Code.PERMISSION_DENIED
                && code != io.grpc.Status.Code.UNAUTHENTICATED) throw e;
          }
        }
      }
    } catch (Exception e) {
      result = 1;
      System.err.println("etcd authentication command failed: " + e.getClass().getSimpleName());
    }
    System.exit(result);
  }

  static Client operatorClient(String[] endpoints, String password) throws Exception {
    Client authenticated =
        Client.builder().endpoints(endpoints).user(bytes("root")).password(bytes(password)).build();
    try {
      waitFor(authenticated.getAuthClient().userList());
      return authenticated;
    } catch (Exception e) {
      authenticated.close();
      boolean disabled = false;
      for (Throwable cause = e; cause != null; cause = cause.getCause()) {
        if ("etcdserver: authentication is not enabled".equals(cause.getMessage())) disabled = true;
      }
      if (!disabled) throw e;
      // Operator provisioning only. The IGinX server never falls back to anonymous access.
      System.err.println(
          "etcd authentication is disabled. Provision accounts, stop IGinX, enable auth, then restart configured nodes (jetcd 0.5.11 requires a maintenance window).");
      return Client.builder().endpoints(endpoints).build();
    }
  }

  public static int run(Auth auth, SecurityConfig security, String action) throws Exception {
    String user = security.get("etcd.user");
    if (user == null || user.equals("root"))
      throw new IllegalArgumentException("Dedicated etcd user required");
    String role = user + "_iginx_metadata";
    List<String> users = waitFor(auth.userList()).getUsers();
    List<String> roles = waitFor(auth.roleList()).getRoles();
    // Never modify a role which has been adopted by an unrelated account.
    if (roles.contains(role)) {
      for (String other : users) {
        if (!other.equals(user) && waitFor(auth.userGet(bytes(other))).getRoles().contains(role))
          throw new IllegalStateException("Managed role is used by another account");
      }
    }
    if (action.equals("revert")) {
      if (users.contains(user) && waitFor(auth.userGet(bytes(user))).getRoles().contains(role))
        waitFor(auth.userRevokeRole(bytes(user), bytes(role)));
      if (roles.contains(role)) {
        for (String other : users) {
          if (!other.equals(user) && waitFor(auth.userGet(bytes(other))).getRoles().contains(role))
            throw new IllegalStateException("Managed role is used by another account");
        }
        waitFor(auth.roleDelete(bytes(role)));
      }
      return 0;
    }
    if (!action.equals("apply") && !action.equals("check"))
      throw new IllegalArgumentException("Unknown action");
    if (action.equals("apply")) {
      if (!users.contains(user))
        waitFor(auth.userAdd(bytes(user), bytes(security.get("etcd.password"))));
      List<String> assigned = waitFor(auth.userGet(bytes(user))).getRoles();
      if (assigned.stream().anyMatch(r -> !r.equals(role)))
        throw new IllegalStateException("Dedicated account has unrelated roles");
      if (!roles.contains(role)) waitFor(auth.roleAdd(bytes(role)));
      Map<String, Permission> actual = new HashMap<>();
      for (Permission permission : waitFor(auth.roleGet(bytes(role))).getPermissions()) {
        actual.put(permissionKey(permission), permission);
      }
      Set<String> expected = expectedPermissions();
      // A repeated apply performs no permission mutations and leaves active clients usable.
      for (String root : metadataRoots()) {
        // Existing etcd keys concatenate these prefixes directly (for example /userroot and
        // /storage0000000001). These are byte prefixes, not ZooKeeper directory boundaries.
        ByteSequence prefix = bytes(root);
        ByteSequence end = OptionsUtil.prefixEndOf(prefix);
        if (!actual.containsKey(root + "|" + end.toString(StandardCharsets.UTF_8) + "|READWRITE"))
          waitFor(auth.roleGrantPermission(bytes(role), prefix, end, Permission.Type.READWRITE));
      }
      for (Map.Entry<String, Permission> entry : actual.entrySet()) {
        if (!expected.contains(entry.getKey())) {
          Permission permission = entry.getValue();
          // Granting READWRITE already replaces a weaker permission for the same key/range.
          String full =
              permission.getKey().toString(StandardCharsets.UTF_8)
                  + "|"
                  + permission.getRangeEnd().toString(StandardCharsets.UTF_8)
                  + "|READWRITE";
          if (!expected.contains(full))
            waitFor(
                auth.roleRevokePermission(
                    bytes(role), permission.getKey(), permission.getRangeEnd()));
        }
      }
      if (!assigned.contains(role)) waitFor(auth.userGrantRole(bytes(user), bytes(role)));
      return 0;
    }
    if (!users.contains(user)
        || !roles.contains(role)
        || !waitFor(auth.userGet(bytes(user))).getRoles().equals(Collections.singletonList(role)))
      return 1;
    Set<String> expected = expectedPermissions();
    Set<String> actual = new HashSet<>();
    for (Permission permission : waitFor(auth.roleGet(bytes(role))).getPermissions()) {
      actual.add(permissionKey(permission));
    }
    return actual.equals(expected) ? 0 : 1;
  }

  private static String permissionKey(Permission permission) {
    return permission.getKey().toString(StandardCharsets.UTF_8)
        + "|"
        + permission.getRangeEnd().toString(StandardCharsets.UTF_8)
        + "|"
        + permission.getPermType();
  }

  private static Set<String> expectedPermissions() {
    Set<String> expected = new HashSet<>();
    for (String root : metadataRoots()) {
      expected.add(
          root
              + "|"
              + OptionsUtil.prefixEndOf(bytes(root)).toString(StandardCharsets.UTF_8)
              + "|READWRITE");
    }
    return expected;
  }
}
