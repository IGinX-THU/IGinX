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

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class SecurityStartupIT {
  @Test(timeout = 45000)
  public void invalidConfigurationsNeverOpenService() throws Exception {
    Path home = Files.createTempDirectory("iginx-invalid-security-");
    String secret = "startup-test-secret-never-log-123456";
    List<String> invalid =
        new ArrayList<>(Arrays.asList("reserved.prefixs=indagent", "reserved.prefixes=indagent"));
    boolean posix = Files.getFileStore(home).supportsFileAttributeView("posix");
    if (posix) invalid.add("internal.secret=" + secret + "\ninternal.auth=required");
    for (int i = 0; i < invalid.size(); i++) {
      int port;
      try (ServerSocket listener = new ServerSocket(0)) {
        port = listener.getLocalPort();
      }
      Path security = home.resolve("security.properties"),
          log = home.resolve("startup-" + i + ".log");
      Files.write(
          home.resolve("config.properties"),
          ("ip=127.0.0.1\nport=" + port).getBytes(StandardCharsets.UTF_8));
      Files.write(security, invalid.get(i).getBytes(StandardCharsets.UTF_8));
      if (posix)
        Files.setPosixFilePermissions(
            security,
            i == 2
                ? EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.OTHERS_READ)
                : EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
      String java =
          Paths.get(
                  System.getProperty("java.home"),
                  "bin",
                  System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
              .toString();
      Process process =
          new ProcessBuilder(
                  java,
                  "-DIGINX_HOME=" + home,
                  "-DIGINX_CONF=" + home.resolve("config.properties"),
                  "-cp",
                  System.getProperty("java.class.path"),
                  "cn.edu.tsinghua.iginx.Iginx")
              .redirectErrorStream(true)
              .redirectOutput(log.toFile())
              .start();
      try {
        assertTrue("Invalid config did not stop startup", process.waitFor(10, TimeUnit.SECONDS));
        assertNotEquals(0, process.exitValue());
        String output = new String(Files.readAllBytes(log), StandardCharsets.UTF_8);
        assertFalse(output.contains(secret));
        assertTrue(output, output.contains(i == 2 ? "must not be readable" : "reserved."));
        try (Socket socket = new Socket()) {
          try {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 500);
            fail("Service opened for invalid config");
          } catch (ConnectException | SocketTimeoutException expected) {
          }
        }
      } finally {
        if (process.isAlive()) process.destroyForcibly();
      }
    }
  }
}
