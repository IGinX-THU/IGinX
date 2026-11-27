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

import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Immutable, process-local secrets. Deliberately separate from the public Config bean. */
public final class SecurityConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecurityConfig.class);
  private static final Set<String> KEYS =
      new HashSet<>(
          Arrays.asList(
              "zookeeper.user",
              "zookeeper.password",
              "zookeeper.acl",
              "etcd.user",
              "etcd.password",
              "internal.secret",
              "internal.auth",
              "reserved.prefixes",
              "reserved.accounts"));
  private final boolean enabled;
  private final Properties properties;
  private final List<String> prefixes;
  private final List<String> accounts;

  private SecurityConfig(boolean enabled, Properties properties) {
    this.enabled = enabled;
    this.properties = properties;
    prefixes = list(properties.getProperty("reserved.prefixes", ""));
    accounts = list(properties.getProperty("reserved.accounts", ""));
    if (!enabled) return;
    for (String key : properties.stringPropertyNames()) {
      require(KEYS.contains(key), "unknown security property: " + key);
      require(!properties.getProperty(key).trim().isEmpty(), "empty security property: " + key);
    }
    pair("zookeeper.user", "zookeeper.password");
    pair("zookeeper.user", "zookeeper.acl");
    pair("etcd.user", "etcd.password");
    pair("internal.secret", "internal.auth");
    for (String key : Arrays.asList("zookeeper.password", "etcd.password", "internal.secret")) {
      require(
          !properties.containsKey(key) || get(key).length() >= 20,
          key + " must have at least 20 characters");
    }
    choice("zookeeper.acl", "open", "creator");
    choice("internal.auth", "optional", "required");
    require(
        prefixes.isEmpty() || !accounts.isEmpty(),
        "reserved.accounts is required with reserved.prefixes");
    for (String prefix : prefixes) {
      require(
          !prefix.contains("*")
              && !prefix.startsWith(".")
              && !prefix.endsWith(".")
              && !prefix.contains("..")
              && !prefix.contains("`")
              && !prefix.contains("{"),
          "reserved.prefixes must contain literal, canonical paths");
    }
  }

  private static final class Holder {
    private static final SecurityConfig INSTANCE = load(defaultPath());
  }

  public static SecurityConfig getInstance() {
    return Holder.INSTANCE;
  }

  public static Path defaultPath() {
    Path config = Paths.get(EnvUtils.loadEnv("IGINX_CONF", "conf/config.properties"));
    if (!config.isAbsolute()) {
      config =
          Paths.get(EnvUtils.loadEnv("IGINX_HOME", System.getProperty("user.dir"))).resolve(config);
    }
    return config.toAbsolutePath().normalize().resolveSibling("security.properties");
  }

  public static SecurityConfig load(Path path) {
    if (Files.notExists(path)) return new SecurityConfig(false, new Properties());
    try {
      if (Files.getFileStore(path).supportsFileAttributeView("posix")) {
        Set<PosixFilePermission> mode = Files.getPosixFilePermissions(path);
        require(
            !mode.contains(PosixFilePermission.GROUP_READ)
                && !mode.contains(PosixFilePermission.OTHERS_READ),
            "security.properties must not be readable by group or others");
      } else {
        LOGGER.warn(
            "Set OS access controls so only the IGinX service account can read security.properties");
      }
      Properties properties = new Properties();
      try (Reader in = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
        properties.load(in);
      }
      return new SecurityConfig(true, properties);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot read security.properties", e);
    }
  }

  private static List<String> list(String value) {
    List<String> result = new ArrayList<>();
    if (!value.trim().isEmpty()) {
      for (String part : value.split(",", -1)) {
        require(!part.trim().isEmpty(), "empty item in security property list");
        result.add(part.trim());
      }
    }
    return Collections.unmodifiableList(result);
  }

  private void pair(String a, String b) {
    require(
        properties.containsKey(a) == properties.containsKey(b),
        a + " and " + b + " must be specified together");
  }

  private void choice(String key, String a, String b) {
    require(
        !properties.containsKey(key) || a.equals(get(key)) || b.equals(get(key)), "invalid " + key);
  }

  private static void require(boolean valid, String message) {
    if (!valid) throw new IllegalArgumentException(message);
  }

  public boolean isEnabled() {
    return enabled;
  }

  public String get(String key) {
    return properties.getProperty(key);
  }

  public List<String> getReservedPrefixes() {
    return prefixes;
  }

  public boolean allowsReserved(String username) {
    if (username == null) return false;
    for (String pattern : accounts) {
      if (globMatches(pattern, username)) return true;
    }
    return false;
  }

  public static boolean globMatches(String pattern, String value) {
    StringBuilder regex = new StringBuilder("^");
    for (String part : pattern.split("\\*", -1)) {
      if (regex.length() > 1) regex.append(".*");
      regex.append(java.util.regex.Pattern.quote(part));
    }
    return value.matches(regex.append('$').toString());
  }
}
