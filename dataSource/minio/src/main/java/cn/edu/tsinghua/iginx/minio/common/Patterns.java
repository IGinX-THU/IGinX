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

import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Column patterns of IGinX, like {@code a.b.*}. A null pattern list means all columns. */
public final class Patterns {

  private static final String STAR = "*";

  private static final List<String> ALL = Collections.singletonList(STAR);

  private Patterns() {}

  public static List<String> all() {
    return ALL;
  }

  public static List<String> nullToAll(@Nullable List<String> patterns) {
    return patterns == null ? ALL : patterns;
  }

  public static boolean isAll(@Nullable Collection<String> patterns) {
    return patterns == null || patterns.contains(STAR);
  }

  public static boolean isWildcard(String pattern) {
    return pattern.contains(STAR);
  }

  public static boolean match(Collection<String> patterns, String name) {
    if (isAll(patterns) || patterns.contains(name)) {
      return true;
    }
    return patterns.stream().anyMatch(pattern -> StringUtils.match(name, pattern));
  }

  /** whether columns matched by pattern may be under prefix */
  public static boolean startsWith(String pattern, @Nullable String prefix) {
    String patternPrefix = IginxPaths.toStringPrefix(pattern);
    String prefixPrefix = IginxPaths.toStringPrefix(prefix);
    if (patternPrefix.startsWith(prefixPrefix)) {
      return true;
    }
    int common = commonPrefixLength(patternPrefix, prefixPrefix);
    return pattern.substring(common).startsWith(STAR);
  }

  @Nullable
  public static List<String> filterByPrefix(@Nullable List<String> patterns, String prefix) {
    if (patterns == null) {
      return null;
    }
    return patterns.stream()
        .filter(pattern -> startsWith(pattern, prefix))
        .collect(Collectors.toList());
  }

  /** the part of pattern after prefix, pattern must start with prefix */
  @Nullable
  public static String suffix(String pattern, @Nullable String prefix) {
    if (prefix == null) {
      return pattern;
    }
    String prefixPrefix = IginxPaths.toStringPrefix(prefix);
    String patternPrefix = IginxPaths.toStringPrefix(pattern);

    int star = patternPrefix.indexOf(STAR);
    star = star == -1 ? patternPrefix.length() : star;
    String beforeWildcard = patternPrefix.substring(0, star);
    String wildcardSuffix = patternPrefix.substring(star);

    if (beforeWildcard.startsWith(prefixPrefix)) {
      return IginxPaths.fromStringPrefix(patternPrefix.substring(prefixPrefix.length()));
    }
    if (prefixPrefix.startsWith(beforeWildcard) && !wildcardSuffix.isEmpty()) {
      return IginxPaths.fromStringPrefix(wildcardSuffix);
    }
    throw new IllegalArgumentException(pattern + " does not start with " + prefix);
  }

  private static int commonPrefixLength(String a, String b) {
    int length = Math.min(a.length(), b.length());
    for (int i = 0; i < length; i++) {
      if (a.charAt(i) != b.charAt(i)) {
        return i;
      }
    }
    return length;
  }
}
