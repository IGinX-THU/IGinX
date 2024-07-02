package cn.edu.tsinghua.iginx.utils;

import static cn.edu.tsinghua.iginx.utils.TimeUtils.convertDatetimeStrToLong;

import java.time.DateTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
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
    Pattern pattern = Pattern.compile(toRegexExpr(iginxPattern));
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
}
