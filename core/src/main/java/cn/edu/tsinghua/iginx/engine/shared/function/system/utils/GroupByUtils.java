/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
