package cn.edu.tsinghua.iginx.engine.shared.function.system.utils;

import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class GroupByUtils {

  public static String transformPath(String path, List<Integer> groupByLevels) {
    String[] levels = path.split("\\" + Constants.LEVEL_SEPARATOR);
    boolean[] retain = new boolean[levels.length];
    for (int groupByLevel : groupByLevels) {
      if (groupByLevel < levels.length) {
        retain[groupByLevel] = true;
      }
    }
    for (int i = 0; i < levels.length; i++) {
      if (!retain[i]) {
        levels[i] = Constants.LEVEL_PLACEHOLDER;
      }
    }
    return String.join(Constants.LEVEL_SEPARATOR, levels);
  }

  public static List<Integer> parseLevelsFromValue(Value value) {
    if (value.getDataType() != DataType.BINARY) {
      throw new IllegalArgumentException(
          "unknown expected datatype for value: " + value.getDataType());
    }
    return Arrays.stream(value.getBinaryVAsString().split(","))
        .map(Integer::parseInt)
        .collect(Collectors.toList());
  }
}
