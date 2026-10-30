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
package cn.edu.tsinghua.iginx.minio.common;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.ESCAPED_DOT;

import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Conversion between IGinX paths and object paths. A dot in an object name is written as {@code
 * dot} (default {@code \.}) in IGinX paths, so that it is not taken as a separator.
 */
public final class IginxPaths {

  /** stands for an escaped dot while splitting a path */
  private static final String DOT_PLACEHOLDER = "\uF000";

  private static final Pattern SPLITTER = Pattern.compile(Pattern.quote(DOT));

  private IginxPaths() {}

  /** join non-null nodes with dot, null if all nodes are null */
  @Nullable
  public static String join(String... nodes) {
    List<String> nonNull =
        Arrays.stream(nodes).filter(Objects::nonNull).collect(Collectors.toList());
    return nonNull.isEmpty() ? null : String.join(DOT, nonNull);
  }

  /** IGinX path of a relative object path */
  public static String get(Path path, String dot) {
    List<String> nodes = new ArrayList<>();
    for (Path node : path) {
      nodes.add(node.toString().replace(DOT, dot));
    }
    return String.join(DOT, nodes);
  }

  /** relative object path of an IGinX path */
  public static Path toFilePath(@Nullable String path, String dot, FileSystem fs) {
    if (path == null) {
      return fs.getPath("");
    }
    String[] nodes = SPLITTER.split(unescape(path));
    for (int i = 0; i < nodes.length; i++) {
      nodes[i] = nodes[i].replace(DOT_PLACEHOLDER, DOT);
    }
    return fs.getPath(nodes[0], Arrays.copyOfRange(nodes, 1, nodes.length));
  }

  /** split an IGinX path into nodes, escaped dots are kept in nodes */
  public static String[] split(@Nullable String path) {
    if (path == null) {
      return new String[0];
    }
    return Arrays.stream(SPLITTER.split(unescape(path)))
        .map(node -> node.replace("\\", "\\\\").replace(DOT_PLACEHOLDER, ESCAPED_DOT))
        .toArray(String[]::new);
  }

  /** "a.b" to "a.b.", null to "" */
  public static String toStringPrefix(@Nullable String path) {
    return path == null ? "" : path + DOT;
  }

  /** inverse of {@link #toStringPrefix} */
  @Nullable
  public static String fromStringPrefix(String prefix) {
    if (prefix.isEmpty()) {
      return null;
    }
    if (!prefix.endsWith(DOT)) {
      throw new IllegalArgumentException("not empty string prefix must end with a dot");
    }
    return prefix.substring(0, prefix.length() - DOT.length());
  }

  /** "\." to placeholder, "\\" to "\", other escapes are kept as is */
  private static String unescape(String path) {
    StringBuilder target = new StringBuilder(path.length());
    boolean escaping = false;
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (!escaping) {
        if (c == '\\') {
          escaping = true;
        } else {
          target.append(c);
        }
        continue;
      }
      switch (c) {
        case '.':
          target.append(DOT_PLACEHOLDER);
          break;
        case '\\':
          target.append('\\');
          break;
        default:
          target.append('\\').append(c);
      }
      escaping = false;
    }
    if (escaping) {
      target.append('\\');
    }
    return target.toString();
  }
}
