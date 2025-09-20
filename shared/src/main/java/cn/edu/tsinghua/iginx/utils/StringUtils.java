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
package cn.edu.tsinghua.iginx.utils;

import static cn.edu.tsinghua.iginx.utils.TimeUtils.convertDatetimeStrToLong;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StringUtils {

  private static final Character MAX_CHAR = '~';
  private static final String SPECIAL_CHAR_SET =
      "[!&()+=|'%`;,<>?\n\t]|[\u2E80\u2E81\u2E82\u2E83\u2E84\u2E85]|[\\x00-\\x1F\\x7F]";
  private static final String[] REGEX = {"[", "]", "(", ")", "|", "+", "?", "*", "-"};

  /**
   * @param ts 时间序列(可能等于/含有*，不可能为null)
   * @param border 分片的开始/结束边界(不可能等于/含有*，可能为null)
   * @param isStart 是否为开始边界
   */
  public static int compare(String ts, String border, boolean isStart) {
    if (border == null) {
      return isStart ? 1 : -1;
    }
    if (ts.equals("*")) {
      return isStart ? 1 : -1;
    }
    if (ts.contains("*")) {
      String p1 = ts.substring(0, ts.indexOf("*"));
      if (border.equals(p1)) {
        return 1;
      }
      if (border.startsWith(p1)) {
        return isStart ? 1 : -1;
      }
      return p1.compareTo(border);
    } else {
      return ts.compareTo(border);
    }
  }

  /**
   * @param ts 时间序列(可能等于/含有*，不可能为null)
   * @param border 前缀式时间范围
   * @return 返回值为 0 表示包含，>0 表示在这个序列在 border 前，<0 表示 ts 在 border 后
   */
  public static int compare(String ts, String border) {
    // *.*.*
    String regex = "[*][.*]*";
    if (Pattern.matches(regex, ts)) return 0;
    // a.b.c.*.*
    String tss = ts;
    if (tss.contains("*")) tss = ts.substring(0, ts.indexOf("*") - 1);
    if (tss.indexOf(border) == 0) {
      return 0;
    } else return tss.compareTo(border);
  }

  public static String nextString(String str) {
    return str + MAX_CHAR;
  }

  public static boolean allHasMoreThanOneSubPath(List<String> pathList) {
    for (String path : pathList) {
      if (!hasMoreThanOneSubPath(path)) {
        return false;
      }
    }
    return true;
  }

  public static boolean hasMoreThanOneSubPath(String path) {
    return path.contains(".");
  }

  public static boolean isPattern(String path) {
    return path.contains("*");
  }

  public static List<String> reformatPaths(List<String> paths) {
    List<String> ret = new ArrayList<>();
    paths.forEach(path -> ret.add(reformatPath(path)));
    return ret;
  }

  public static String reformatPath(String path) {
    path = path.replaceAll("[.^${}+?()\\[\\]|\\\\]", "\\\\$0");
    if (!path.contains("*")) {
      return path;
    }
    path = path.replaceAll("[*]", ".*");
    return path;
  }

  public static boolean match(String string, String iginxPattern) {
    return toColumnMatcher(iginxPattern).test(string);
  }

  public static Predicate<String> toColumnMatcher(String iginxPattern) {
    Objects.requireNonNull(iginxPattern);

    if (iginxPattern.equals("*")) {
      return s -> true;
    }
    if (!iginxPattern.contains("*")) {
      return iginxPattern::equals;
    }
    Pattern pattern = Pattern.compile(toRegexExpr(iginxPattern), Pattern.DOTALL);
    return s -> pattern.matcher(s).matches();
  }

  public static String toRegexExpr(String iginxPattern) {
    Objects.requireNonNull(iginxPattern);

    return Arrays.stream(iginxPattern.split("[*]+", -1))
        .map(s -> s.isEmpty() ? "" : Pattern.quote(s))
        .collect(Collectors.joining(".*"));
  }

  public static boolean isContainSpecialChar(String str) {
    String regEx = SPECIAL_CHAR_SET;
    Pattern p = Pattern.compile(regEx);
    Matcher m = p.matcher(str);
    return m.find();
  }

  // 是否包含除IGinX认同的合法字符之外的，正则表达式字符
  public static boolean isContainRegex(String str) {
    if (str == null) {
      return false;
    }
    for (int i = 0; i < REGEX.length; i++) {
      if (str.contains(REGEX[i])) {
        return true;
      }
    }
    return false;
  }

  public static boolean isEqual(String str1, String str2) {
    if ((str1 == null || str1.isEmpty()) && (str2 == null || str2.isEmpty())) {
      return true;
    }
    return Objects.equals(str1, str2);
  }

  public static String replaceSpecialCharsWithUnderscore(String str) {
    String regex = SPECIAL_CHAR_SET + "|[\" \\.\\*\\[\\]\\-]";
    return str.replaceAll(regex, "_");
  }

  // will only convert integers or time to long
  public static Long tryParse2Key(String value) {
    Long res = null;
    try {
      res = Long.parseLong(value);
    } catch (NumberFormatException e) {
      // now try for time
      try {
        res = convertDatetimeStrToLong(value);
      } catch (DateTimeException ex) {
        throw ex;
      }
    }
    return res;
  }

  public static Set<String> cutSchemaPrefix(String schemaPrefix, Set<String> patterns) {
    // show columns的patterns为空，查询所有列
    if (patterns.isEmpty()) {
      return Collections.singleton("*");
    }
    // 该数据源没有schema prefix，直接匹配patterns
    if (schemaPrefix == null || schemaPrefix.isEmpty()) {
      return patterns;
    }

    Set<String> patternsCutSchemaPrefix = new HashSet<>();
    for (String pattern : patterns) {
      Set<String> tmp = cutSchemaPrefix(schemaPrefix, pattern);
      if (tmp.contains("*")) {
        return Collections.singleton("*");
      }
      patternsCutSchemaPrefix.addAll(tmp);
    }
    return patternsCutSchemaPrefix;
  }

  private static Set<String> cutSchemaPrefix(String schemaPrefix, String pattern) {
    String[] prefixSplit = schemaPrefix.split("\\.");
    String[] patternSplit = pattern.split("\\.");
    int minLen = Math.min(prefixSplit.length, patternSplit.length);
    int index = 0;
    // 逐级匹配pattern和schemaPrefix
    while (index < minLen && prefixSplit[index].equals(patternSplit[index])) {
      index++;
    }

    // pattern匹配结束，schemaPrefix还有剩余，则该storageEngine下没有该pattern
    if (index == patternSplit.length) {
      return Collections.emptySet();
    }

    // schemaPrefix匹配结束，pattern还有剩余，则把该pattern减去前缀schemaPrefix
    if (index == prefixSplit.length) {
      return Collections.singleton(joinWithDot(patternSplit, index));
    }

    // pattern和schemaPrefix不匹配
    if (!patternSplit[index].equals("*")) {
      return Collections.emptySet();
    }

    Set<String> target = new HashSet<>();
    // 将pattern的'*'视为部分匹配该前缀，即把'*'下推到数据源
    target.add(joinWithDot(patternSplit, index));
    if (index + 1 < patternSplit.length) {
      // 将pattern的'*'视为完全匹配该前缀，即不把'*'下推到数据源
      String patternRemain = joinWithDot(patternSplit, index + 1);
      target.add(patternRemain);

      // 将schemaPrefix的每一级分别匹配'*'
      for (int i = index + 1; i < prefixSplit.length; i++) {
        String prefixRemain = joinWithDot(prefixSplit, i);
        target.addAll(cutSchemaPrefix(prefixRemain, patternRemain));
      }
    }

    return target;
  }

  private static String joinWithDot(String[] strings, int begin) {
    if (begin >= strings.length) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    for (int i = begin; i < strings.length; i++) {
      sb.append(strings[i]).append(".");
    }
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  public static Set<String> intersectDataPrefix(String dataPrefix, Set<String> patterns) {
    if (dataPrefix == null || dataPrefix.isEmpty() || patterns.isEmpty()) {
      return patterns;
    }
    String dataPrefixRegex = dataPrefix + ".*";
    if (patterns.contains("*")) {
      return Collections.singleton(dataPrefixRegex);
    }

    Set<String> target = new HashSet<>();
    for (String pattern : patterns) {
      Set<String> tmp = intersectDataPrefix(dataPrefix, pattern);
      if (tmp == null) {
        continue;
      }
      if (tmp.contains(dataPrefixRegex)) {
        return Collections.singleton(dataPrefixRegex);
      }
      target.addAll(tmp);
    }

    // 移除不必要的pattern
    return mergePatterns(target);
  }

  private static Set<String> intersectDataPrefix(String dataPrefix, String pattern) {
    String[] prefixSplit = dataPrefix.split("\\.");
    String[] patternSplit = pattern.split("\\.");
    StringBuilder commonPrefix = new StringBuilder();
    int minLen = Math.min(prefixSplit.length, patternSplit.length);
    int index = 0;
    // 逐级匹配pattern和dataPrefix
    while (index < minLen && prefixSplit[index].equals(patternSplit[index])) {
      commonPrefix.append(prefixSplit[index]);
      index++;
    }

    // pattern匹配结束，dataPrefix还有剩余，则交集取dataPrefix
    if (index == patternSplit.length) {
      return Collections.singleton(dataPrefix + ".*");
    }

    // dataPrefix匹配结束，pattern还有剩余，则交集取pattern
    if (index == prefixSplit.length) {
      return Collections.singleton(pattern);
    }

    // pattern和dataPrefix不匹配
    if (!patternSplit[index].equals("*")) {
      return Collections.emptySet();
    }

    Set<String> target = new HashSet<>();
    // 将pattern的'*'视为部分匹配该前缀，即把'*'下推到数据源
    target.add(dataPrefix + "." + joinWithDot(patternSplit, index));
    if (index + 1 < patternSplit.length) {
      // 将pattern的'*'视为完全匹配该前缀，即不把'*'下推到数据源
      String patternRemain = joinWithDot(patternSplit, index + 1);
      target.add(dataPrefix + "." + patternRemain);

      if (commonPrefix.length() > 0) {
        commonPrefix.append(".");
      }
      // 将dataPrefix的每一级分别匹配'*'
      for (int i = index + 1; i < prefixSplit.length; i++) {
        commonPrefix.append(prefixSplit[i - 1]).append(".");
        String prefixRemain = joinWithDot(prefixSplit, i);
        Set<String> ret = intersectDataPrefix(prefixRemain, patternRemain);
        ret.forEach(str -> target.add(commonPrefix + str));
      }
    }

    return target;
  }

  private static Set<String> mergePatterns(Set<String> patterns) {
    if (patterns.size() <= 1) {
      return patterns;
    }
    Set<String> target = new HashSet<>();
    List<String> list = new ArrayList<>(patterns);
    int size = list.size();
    List<Integer> toBeRemoved = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      boolean removed = false;
      for (int j = 0; j != i && !toBeRemoved.contains(j) && j < size; j++) {
        // 第j个pattern包含第i个pattern
        if (match(list.get(i), list.get(j))) {
          toBeRemoved.add(i);
          removed = true;
          break;
        }
      }
      if (!removed) {
        target.add(list.get(i));
      }
    }
    return target;
  }

  public static String getLongestCommonPrefix(List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return "";
    }
    int minLen = Integer.MAX_VALUE;
    for (String str : patterns) {
      minLen = Math.min(minLen, str.length());
    }
    int low = 0, high = minLen;
    while (low < high) {
      int mid = (high - low + 1) / 2 + low;
      if (isCommonPrefix(patterns, mid)) {
        low = mid;
      } else {
        high = mid - 1;
      }
    }
    String prefix = patterns.get(0).substring(0, low);
    int lastDotIndex = prefix.lastIndexOf(".");
    return lastDotIndex != -1 ? prefix.substring(0, lastDotIndex) : "";
  }

  private static boolean isCommonPrefix(List<String> patterns, int length) {
    String prefix = patterns.get(0).substring(0, length);
    for (String str : patterns) {
      if (!str.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }

  public static boolean hasCommonPrefix(String prefix, String pattern) {
    while (!pattern.startsWith(prefix)) {
      int lastDotIndex = prefix.lastIndexOf(".");
      if (lastDotIndex != -1) {
        prefix = prefix.substring(0, lastDotIndex);
      } else {
        return false;
      }
    }
    return true;
  }
}
