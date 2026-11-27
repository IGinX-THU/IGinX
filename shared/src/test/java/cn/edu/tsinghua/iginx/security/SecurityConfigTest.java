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
package cn.edu.tsinghua.iginx.security;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import org.junit.Test;

public class SecurityConfigTest {
  private Path file(String content) throws Exception {
    Path file = Files.createTempFile("iginx-security-", ".properties");
    if (Files.getFileStore(file).supportsFileAttributeView("posix"))
      Files.setPosixFilePermissions(
          file, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    file.toFile().deleteOnExit();
    return file;
  }

  @Test
  public void absentFileMeansDisabled() throws Exception {
    assertFalse(
        SecurityConfig.load(
                Files.createTempDirectory("absent-security-").resolve("security.properties"))
            .isEnabled());
    SecurityConfig empty = SecurityConfig.load(file(""));
    assertTrue(empty.isEnabled());
    assertNull(empty.get("internal.auth"));
    assertTrue(empty.getReservedPrefixes().isEmpty());
    assertNull(empty.get("internal.secret"));
    assertNull(empty.get("zookeeper.user"));
    SecurityConfig zkOnly =
        SecurityConfig.load(
            file(
                "zookeeper.user=meta\nzookeeper.password=123456789012345678901234\nzookeeper.acl=open"));
    assertTrue(zkOnly.isEnabled());
    assertTrue(zkOnly.getReservedPrefixes().isEmpty());
    assertNull(zkOnly.get("internal.secret"));
    assertNull(zkOnly.get("internal.auth"));
  }

  @Test
  public void parseAndValidate() throws Exception {
    String secret = "a-secret-of-at-least-twenty-characters";
    SecurityConfig valid =
        SecurityConfig.load(
            file(
                "zookeeper.user=meta\nzookeeper.password="
                    + secret
                    + "\nzookeeper.acl=creator\ninternal.secret="
                    + secret
                    + "\ninternal.auth=required\nreserved.prefixes=indagent,sys.blob\nreserved.accounts=indagent_data_*,blob_*\n"));
    assertEquals(secret, valid.get("internal.secret"));
    assertEquals(Arrays.asList("indagent", "sys.blob"), valid.getReservedPrefixes());
    for (String value :
        Arrays.asList(
            "zookeeper.user=u",
            "zookeeper.password=" + secret,
            "zookeeper.acl=open",
            "etcd.user=u",
            "internal.secret=" + secret,
            "internal.auth=required",
            "zookeeper.user=u\nzookeeper.password=short\nzookeeper.acl=open",
            "zookeeper.user=u\nzookeeper.password=" + secret,
            "zookeeper.user=u\nzookeeper.password=" + secret + "\nzookeeper.acl=typo",
            "internal.secret=" + secret + "\ninternal.auth=typo",
            "internal.secret=short\ninternal.auth=optional",
            "reserved.prefixes=indagent",
            "reserved.prefxies=indagent",
            "reserved.accounts=",
            "reserved.accounts=x,,y")) {
      try {
        SecurityConfig.load(file(value));
        fail("Accepted invalid configuration");
      } catch (IllegalArgumentException e) {
        assertFalse(e.getMessage().contains(secret));
      }
    }
  }

  @Test
  public void allowlistBoundaries() throws Exception {
    SecurityConfig config =
        SecurityConfig.load(
            file("reserved.prefixes=indagent\nreserved.accounts=indagent_data_*,blob_*"));
    for (String name : Arrays.asList("indagent_data_v1", "indagent_data_", "blob_2"))
      assertTrue(config.allowsReserved(name));
    for (String name :
        Arrays.asList("root", "x_indagent_data_1", "indagent_dat", "INDAGENT_DATA_1"))
      assertFalse(config.allowsReserved(name));
    assertFalse(config.allowsReserved(null));
  }

  @Test
  public void permissionsAndEnvironment() throws Exception {
    Path path = file("internal.secret=real-secret-at-least-20-characters\ninternal.auth=optional");
    System.setProperty("internal.secret", "overridden");
    try {
      assertEquals(
          "real-secret-at-least-20-characters", SecurityConfig.load(path).get("internal.secret"));
    } finally {
      System.clearProperty("internal.secret");
    }
    String oldHome = System.getProperty("IGINX_HOME"), oldConf = System.getProperty("IGINX_CONF");
    try {
      System.setProperty("IGINX_HOME", path.getParent().toString());
      System.clearProperty("IGINX_CONF");
      assertEquals(
          path.getParent().resolve("conf/security.properties"), SecurityConfig.defaultPath());
      System.setProperty(
          "IGINX_CONF", path.getParent().resolve("custom/config.properties").toString());
      assertEquals(
          path.getParent().resolve("custom/security.properties"), SecurityConfig.defaultPath());
    } finally {
      if (oldHome == null) System.clearProperty("IGINX_HOME");
      else System.setProperty("IGINX_HOME", oldHome);
      if (oldConf == null) System.clearProperty("IGINX_CONF");
      else System.setProperty("IGINX_CONF", oldConf);
    }
    if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
      Files.setPosixFilePermissions(
          path, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ));
      try {
        SecurityConfig.load(path);
        fail("Accepted group-readable secrets");
      } catch (IllegalArgumentException expected) {
      }
    }
  }
}
