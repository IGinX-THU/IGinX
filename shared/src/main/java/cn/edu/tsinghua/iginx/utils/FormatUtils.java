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
package cn.edu.tsinghua.iginx.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class FormatUtils {

  public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  public static String formatResult(List<List<String>> result) {
    if (result.isEmpty()) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    List<Integer> maxSizeList = new ArrayList<>();

    int colCount = result.get(0).size();
    for (int i = 0; i < colCount; i++) {
      maxSizeList.add(0);
    }
    for (List<String> row : result) {
      for (int i = 0; i < colCount; i++) {
        maxSizeList.set(i, Math.max(row.get(i).length(), maxSizeList.get(i)));
      }
    }

    builder.append(buildBlockLine(maxSizeList));
    builder.append(buildRow(result, 0, maxSizeList));
    builder.append(buildBlockLine(maxSizeList));
    for (int i = 1; i < result.size(); i++) {
      builder.append(buildRow(result, i, maxSizeList));
    }
    builder.append(buildBlockLine(maxSizeList));
    return builder.toString();
  }

  private static String buildBlockLine(List<Integer> maxSizeList) {
    StringBuilder blockLine = new StringBuilder();
    for (Integer integer : maxSizeList) {
      blockLine.append("+").append(StringUtils.repeat("-", integer));
    }
    blockLine.append("+").append("\n");
    return blockLine.toString();
  }

  private static String buildRow(List<List<String>> cache, int rowIdx, List<Integer> maxSizeList) {
    StringBuilder builder = new StringBuilder();
    builder.append("|");
    int maxSize;
    String rowValue;
    for (int i = 0; i < maxSizeList.size(); i++) {
      maxSize = maxSizeList.get(i);
      rowValue = cache.get(rowIdx).get(i);
      builder.append(String.format("%" + maxSize + "s|", rowValue));
    }
    builder.append("\n");
    return builder.toString();
  }

  public static String formatCount(int count) {
    if (count <= 0) {
      return "Empty set.\n";
    } else {
      return "Total line number = " + count + "\n";
    }
  }

  public static String formatTime(long timestamp, String timeFormat, String timePrecision) {
    long timeInMs = TimeUtils.getTimeInNs(timestamp, TimeUtils.strToTimePrecision(timePrecision));
    return new SimpleDateFormat(timeFormat).format(timeInMs);
  }

  public static String valueToString(Object value) {
    String ret;
    if (value instanceof byte[]) {
      ret = new String((byte[]) value);
    } else {
      ret = String.valueOf(value);
    }
    return ret;
  }
}
