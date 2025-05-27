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

import cn.edu.tsinghua.iginx.utils.StringUtils;
import com.google.common.base.Strings;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class Patterns {
  private Patterns() {}

  public static final String STAR = "*";

  public static boolean isAll(String pattern) {
    return pattern.equals(STAR);
  }

  public static boolean isAll(@Nullable Collection<String> patterns) {
    if (patterns == null) {
      return true;
    }
    return patterns.stream().anyMatch(Patterns::isAll);
  }

  public static List<String> filterByPrefix(
      @Nullable List<String> patterns, @Nullable String subPrefix) {
    if (patterns == null || subPrefix == null) {
      return patterns;
    }
    return patterns.stream()
        .filter(pattern -> startsWith(pattern, subPrefix))
        .collect(Collectors.toList());
  }

  public static boolean startsWith(String pattern, @Nullable String prefix) {
    String patternStringPrefix = IginxPaths.toStringPrefix(pattern);
    String prefixStringPrefix = IginxPaths.toStringPrefix(prefix);

    if (patternStringPrefix.startsWith(prefixStringPrefix)) {
      return true;
    }

    String commonPrefix = Strings.commonPrefix(patternStringPrefix, prefixStringPrefix);
    String patternWithoutCommonPrefix = pattern.substring(commonPrefix.length());
    return patternWithoutCommonPrefix.startsWith(STAR);
  }

  public static boolean startsWith(@Nullable List<String> patterns, @Nullable String subPrefix) {
    if (patterns == null || subPrefix == null) {
      return true;
    }
    return patterns.stream().anyMatch(pattern -> startsWith(pattern, subPrefix));
  }

  public static String removePrefix(String pattern, @Nullable String prefix) {
    if (prefix == null) {
      return pattern;
    }
    String prefixStringPrefix = IginxPaths.toStringPrefix(prefix);
    String patternStringPrefix = IginxPaths.toStringPrefix(pattern);

    if (patternStringPrefix.startsWith(prefixStringPrefix)) {
      return IginxPaths.fromStringPrefix(
          patternStringPrefix.substring(prefixStringPrefix.length()));
    } else {
      String commonPrefix = Strings.commonPrefix(patternStringPrefix, prefixStringPrefix);
      String patternWithoutCommonPrefix = patternStringPrefix.substring(commonPrefix.length());
      if (patternWithoutCommonPrefix.startsWith(STAR)) {
        return IginxPaths.fromStringPrefix(patternWithoutCommonPrefix);
      } else {
        throw new IllegalArgumentException(pattern + " does not start with " + prefix);
      }
    }
  }

  private static final List<String> ALL = Collections.singletonList(STAR);

  public static List<String> all() {
    return ALL;
  }

  public static List<String> nonNull(@Nullable List<String> patterns) {
    return patterns == null ? ALL : patterns;
  }

  public static boolean match(Collection<String> patterns, String name) {
    if (Patterns.isAll(patterns)) {
      return true;
    }
    if (patterns.contains(name)) {
      return true;
    }
    return patterns.stream().anyMatch(pattern -> match(pattern, name));
  }

  public static boolean match(String patterns, String name) {
    return StringUtils.match(name, patterns);
  }

  public static boolean isWildcard(String path) {
    return path.contains(STAR);
  }

  public static boolean isEmpty(@Nullable List<String> subPatterns) {
    if (subPatterns == null) {
      return false;
    }
    return subPatterns.isEmpty();
  }

  public static List<String> nullToAll(@Nullable List<String> patterns) {
    return patterns == null ? all() : patterns;
  }

  public static String suffix(String pattern, @Nullable String prefix) {
    if (prefix == null) {
      return pattern;
    }
    String stringPrefix = IginxPaths.toStringPrefix(prefix);
    String patternStringPrefix = IginxPaths.toStringPrefix(pattern);

    int starIndex = patternStringPrefix.indexOf(STAR);
    starIndex = starIndex == -1 ? patternStringPrefix.length() : starIndex;
    String beforeWildcard = patternStringPrefix.substring(0, starIndex);
    String wildcardSuffix = patternStringPrefix.substring(starIndex);

    if (beforeWildcard.startsWith(stringPrefix)) {
      return IginxPaths.fromStringPrefix(patternStringPrefix.substring(stringPrefix.length()));
    } else {
      if (stringPrefix.startsWith(beforeWildcard)) {
        if (!wildcardSuffix.isEmpty()) {
          return IginxPaths.fromStringPrefix(wildcardSuffix);
        }
      }
      throw new IllegalArgumentException(pattern + " does not start with " + prefix);
    }
  }
}
