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
package cn.edu.tsinghua.iginx.filesystem.common;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.DOT;
import static cn.edu.tsinghua.iginx.constant.GlobalConstant.ESCAPED_DOT;

import cn.edu.tsinghua.iginx.filesystem.struct.legacy.filesystem.tools.FilePathUtils;
import com.google.common.collect.Iterables;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public class IginxPaths {

  private IginxPaths() {}

  @Nullable
  public static String join(String... paths) {
    return join(Arrays.asList(paths));
  }

  @Nullable
  public static String join(Iterable<? extends CharSequence> paths) {
    Iterable<? extends CharSequence> nonNullPaths = Iterables.filter(paths, Objects::nonNull);
    if (!nonNullPaths.iterator().hasNext()) {
      return null;
    }
    return String.join(DOT, nonNullPaths);
  }

  public static String get(Path path, String dot) {
    List<String> nodes = new ArrayList<>();
    for (Path fsNode : path) {
      nodes.add(fsNode.toString().replace(DOT, dot));
    }
    return join(nodes);
  }

  public static Path toFilePath(@Nullable String path, String dot, FileSystem fs) {
    if (path == null) {
      return fs.getPath("");
    }
    String safePath = FilePathUtils.unescapePath(path);
    Pattern splitter = Pattern.compile(Pattern.quote(DOT));
    String[] nodes = splitter.split(safePath);
    String[] fsNodes = new String[nodes.length];
    for (int i = 0; i < nodes.length; i++) {
      fsNodes[i] = nodes[i].replace(FilePathUtils.DOT_PLACEHOLDER, DOT);
    }
    return fs.getPath(fsNodes[0], Arrays.copyOfRange(fsNodes, 1, fsNodes.length));
  }

  public static String toStringPrefix(@Nullable String path) {
    if (path == null) {
      return "";
    } else {
      return path + DOT;
    }
  }

  @Nullable
  public static String fromStringPrefix(String path) {
    if (path.isEmpty()) {
      return null;
    } else {
      if (!path.endsWith(DOT)) {
        throw new IllegalArgumentException("not empty string prefix must not end with a dot");
      }
      return path.substring(0, path.length() - DOT.length());
    }
  }

  public static String[] split(@Nullable String path) {
    if (path == null) {
      return new String[0];
    }
    String safePath = FilePathUtils.unescapePath(path);
    Pattern splitter = Pattern.compile(Pattern.quote(DOT));
    return Arrays.stream(splitter.split(safePath))
        .map(
            s -> {
              s = s.replace("\\", "\\\\");
              s = s.replace(FilePathUtils.DOT_PLACEHOLDER, ESCAPED_DOT);
              return s;
            })
        .toArray(String[]::new);
  }
}
