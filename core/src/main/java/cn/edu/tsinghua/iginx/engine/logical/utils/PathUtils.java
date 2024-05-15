package cn.edu.tsinghua.iginx.engine.logical.utils;

import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PathUtils {

  public static final String STAR = "*";

  public static final Character MIN_CHAR = '!';
  public static final Character MAX_CHAR = '~';

  public static ColumnsInterval trimColumnsInterval(ColumnsInterval columnsInterval) {
    String startColumn = columnsInterval.getStartColumn();
    if (startColumn.contains(STAR)) {
      if (startColumn.startsWith(STAR)) {
        startColumn = null;
      } else {
        startColumn = startColumn.substring(0, startColumn.indexOf(STAR)) + MIN_CHAR;
      }
    }

    String endColumn = columnsInterval.getEndColumn();
    if (endColumn.contains(STAR)) {
      if (endColumn.startsWith(STAR)) {
        endColumn = null;
      } else {
        endColumn = endColumn.substring(0, endColumn.indexOf(STAR)) + MAX_CHAR;
      }
    }

    return new ColumnsInterval(startColumn, endColumn);
  }

  public static ColumnsInterval addSuffix(ColumnsInterval columnsInterval) {
    String startColumn = columnsInterval.getStartColumn();
    String endColumn = columnsInterval.getEndColumn();
    return new ColumnsInterval(startColumn + MIN_CHAR, endColumn + MAX_CHAR);
  }

  /**
   * 反向重命名模式列表中的模式
   *
   * @param aliasMap 重命名规则, key为旧模式，value为新模式，在这里我们要将新模式恢复为旧模式
   * @param patterns 要重命名的模式列表
   * @return 重命名后的模式列表
   */
  public static List<String> recoverRenamedPatterns(
      Map<String, String> aliasMap, List<String> patterns) {
    return patterns.stream()
        .map(pattern -> recoverRenamedPattern(aliasMap, pattern))
        .collect(Collectors.toList());
  }

  public static String recoverRenamedPattern(Map<String, String> aliasMap, String pattern) {
    for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
      String oldPattern = entry.getKey().replace("*", "$1"); // 通配符转换为正则的捕获组
      String newPattern = entry.getValue().replace("*", "(.*)"); // 使用反向引用保留原始匹配的部分
      if (pattern.matches(newPattern)) {
        // 如果旧模式中有通配符，但是新模式中没有，我们需要将新模式中的捕获组替换为通配符
        if (oldPattern.contains("$1") && !newPattern.contains("*")) {
          oldPattern = oldPattern.replace("$1", "*");
        }
        return pattern.replaceAll(newPattern, oldPattern);
      } else if (newPattern.equals(pattern)) {
        return entry.getKey();
      } else if (pattern.contains(".*") && newPattern.matches(StringUtils.reformatPath(pattern))) {
        return entry.getKey();
      }
    }
    return pattern;
  }
}
