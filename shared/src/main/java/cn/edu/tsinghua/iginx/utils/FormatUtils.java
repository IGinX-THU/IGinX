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
        maxSizeList.set(i, Math.max(getDisplayWidth(row.get(i)), maxSizeList.get(i)));
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

  private static int getDisplayWidth(String str) {
    int width = 0;
    for (char c : str.toCharArray()) {
      if (isChinese(c)) {
        width += 2;
      } else {
        width += 1;
      }
    }
    return width;
  }

  private static boolean isChinese(char c) {
    Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
    return ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
        || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
        || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION
        || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION
        || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS;
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
      int padding = maxSize - getDisplayWidth(rowValue);
      for (int j = 0; j < padding; j++) {
        builder.append(' ');
      }
      builder.append(rowValue).append('|');
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
