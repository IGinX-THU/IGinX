package cn.edu.tsinghua.iginx.filestore.common;

import javax.annotation.Nullable;
import java.util.Collection;

public class Patterns {
  private Patterns() {
  }

  public static boolean isAll(String pattern) {
    return pattern.equals("*");
  }
  
  public static boolean isAll(@Nullable Collection<String> patterns) {
    if (patterns == null) {
      return true;
    }
    return patterns.stream().anyMatch(Patterns::isAll);
  }
}
