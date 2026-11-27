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

import cn.edu.tsinghua.iginx.metadata.storage.etcd.EtcdAuthMigration;
import cn.edu.tsinghua.iginx.security.SecurityConfig;
import io.etcd.jetcd.*;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.*;
import org.junit.Test;

/** Starts a private etcd process; select explicitly with -Dtest=EtcdAuthIT. */
public class EtcdAuthIT {
  private static ByteSequence b(String value) {
    return ByteSequence.from(value, StandardCharsets.UTF_8);
  }

  private static <T> T waitFor(CompletableFuture<T> future) throws Exception {
    return future.get(10, TimeUnit.SECONDS);
  }

  private static int port() throws Exception {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  @Test
  public void leastPrivilegeAndIdempotentMigration() throws Exception {
    String executable = System.getProperty("iginx.test.etcd", System.getenv("IGINX_TEST_ETCD"));
    assertNotNull("Set IGINX_TEST_ETCD to an etcd 3.5 executable", executable);
    Path directory = Files.createTempDirectory("iginx-etcd-it-");
    String endpoint = "http://127.0.0.1:" + port(), peer = "http://127.0.0.1:" + port();
    Process process =
        new ProcessBuilder(
                executable,
                "--data-dir",
                directory.resolve("data").toString(),
                "--listen-client-urls",
                endpoint,
                "--advertise-client-urls",
                endpoint,
                "--listen-peer-urls",
                peer,
                "--initial-advertise-peer-urls",
                peer,
                "--initial-cluster",
                "default=" + peer)
            .redirectErrorStream(true)
            .redirectOutput(directory.resolve("etcd.log").toFile())
            .start();
    String password = "test-only-password-123456789";
    Path file = directory.resolve("security.properties");
    Files.write(
        file, ("etcd.user=iginx\netcd.password=" + password).getBytes(StandardCharsets.UTF_8));
    if (Files.getFileStore(file).supportsFileAttributeView("posix"))
      Files.setPosixFilePermissions(
          file, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    SecurityConfig security = SecurityConfig.load(file);
    try (Client open = Client.builder().endpoints(endpoint).build()) {
      waitFor(open.getKVClient().put(b("/other/keep"), b("unchanged")));
      Auth auth = open.getAuthClient();
      waitFor(auth.userAdd(b("root"), b(password)));
      waitFor(auth.userGrantRole(b("root"), b("root")));
      assertEquals(1, EtcdAuthMigration.run(auth, security, "check"));
      assertEquals(0, EtcdAuthMigration.run(auth, security, "apply"));
      assertEquals(0, EtcdAuthMigration.run(auth, security, "apply"));
      // Exact pinned jetcd compatibility before global auth is enabled.
      try (Client configured =
          Client.builder().endpoints(endpoint).user(b("iginx")).password(b(password)).build()) {
        try {
          waitFor(configured.getKVClient().put(b("/user/before"), b("present")));
          fail("Revisit the documented maintenance-window requirement if jetcd changes");
        } catch (ExecutionException expected) {
          assertTrue(
              expected.getCause().getMessage(),
              expected.getCause().getMessage().contains("authentication is not enabled"));
        }
      }
      waitFor(auth.authEnable());
      try (Client root =
              Client.builder().endpoints(endpoint).user(b("root")).password(b(password)).build();
          Client configured =
              Client.builder().endpoints(endpoint).user(b("iginx")).password(b(password)).build()) {
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "check"));
        for (String key :
            Arrays.asList(
                "/user/u", "/lock/user/1", "/id/iginx", "/storage/engine", "/policy/version")) {
          waitFor(configured.getKVClient().put(b(key), b("ok")));
          assertEquals(1, waitFor(configured.getKVClient().get(b(key))).getCount());
        }
        for (String key :
            Arrays.asList("/other/keep", "/indagent/private", "/application-user", "/")) {
          try {
            waitFor(configured.getKVClient().get(b(key)));
            fail("read outside roots " + key);
          } catch (ExecutionException expected) {
          }
          try {
            waitFor(configured.getKVClient().put(b(key), b("denied")));
            fail("write outside roots " + key);
          } catch (ExecutionException expected) {
          }
        }
        try {
          waitFor(open.getKVClient().get(b("/user/u")));
          fail("anonymous read");
        } catch (ExecutionException expected) {
        }
        assertEquals(
            "unchanged",
            waitFor(root.getKVClient().get(b("/other/keep")))
                .getKvs()
                .get(0)
                .getValue()
                .toString(StandardCharsets.UTF_8));
        long revision = waitFor(root.getKVClient().get(b("/user/u"))).getHeader().getRevision();
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "apply"));
        assertEquals(
            revision, waitFor(root.getKVClient().get(b("/user/u"))).getHeader().getRevision());
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "revert"));
        assertEquals(0, EtcdAuthMigration.run(root.getAuthClient(), security, "revert"));
        assertEquals(1, EtcdAuthMigration.run(root.getAuthClient(), security, "check"));
        waitFor(root.getAuthClient().authDisable());
      }
    } finally {
      process.destroy();
      if (!process.waitFor(5, TimeUnit.SECONDS)) process.destroyForcibly();
    }
  }
}
