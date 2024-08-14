package cn.edu.tsinghua.iginx.filestore.common;

import java.util.regex.Pattern;

public class Strings {
  private Strings() {}

  private static final Pattern PATTERN = Pattern.compile("\n");

  public static String shiftWithNewline(String string) {
    return PATTERN.matcher("\n" + string).replaceAll("\n    ");
  }
}
