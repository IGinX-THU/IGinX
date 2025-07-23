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
package cn.edu.tsinghua.iginx.integration.tool;

import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestUtils {
  /**
   * 比较ResultSet的表格内容是否相等，不比较顺序。
   *
   * @param table1 表格1
   * @param table2 表格2
   * @return 是否相等
   */
  public static boolean compareTables(String table1, String table2) {
    List<List<String>> list1 = parseTable(table1);
    List<List<String>> list2 = parseTable(table2);

    list1.sort(new ListComparator());
    list2.sort(new ListComparator());

    return list1.equals(list2);
  }

  /**
   * 将ResultSet的表格内容解析为二维字符串列表。
   *
   * @param table 表格字符串
   * @return 二维字符串列表
   */
  private static List<List<String>> parseTable(String table) {
    List<List<String>> list = new ArrayList<>();
    String[] lines = table.split("\n");

    for (String line : lines) {
      if (line.startsWith("|") && line.endsWith("|")) {
        String[] values = line.split("\\|");
        List<String> row = new ArrayList<>();
        for (String value : values) {
          value = value.trim();
          if (!value.isEmpty()) {
            row.add(value);
          }
        }
        if (!row.isEmpty()) {
          list.add(row);
        }
      }
    }

    return list;
  }

  static class ListComparator implements Comparator<List<String>> {
    @Override
    public int compare(List<String> o1, List<String> o2) {
      for (int i = 0; i < o1.size(); i++) {
        int comparison = o1.get(i).compareTo(o2.get(i));
        if (comparison != 0) {
          return comparison;
        }
      }
      return 0;
    }
  }

  public static void downloadFile(String fileUrl, String destinationFile) throws IOException {
    File file = new File(destinationFile);
    File parentDir = file.getParentFile();
    if (parentDir != null && !parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        throw new IOException("Cannot create dir：" + parentDir);
      }
    }
    URL url = new URL(fileUrl);
    try (ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream out = new FileOutputStream(destinationFile);
        FileChannel fileChannel = out.getChannel()) {
      fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
    }
  }

  public static boolean isResultSetEqual(String expected, String actual) {
    String expectedHeader = getHeader(expected);
    String actualHeader = getHeader(actual);
    Map<String, Integer> expectedCounts = countDataRows(expected);
    Map<String, Integer> actualCounts = countDataRows(actual);
    return Objects.equals(expectedCounts, actualCounts)
        && Objects.equals(expectedHeader, actualHeader);
  }

  private static String getHeader(String tableStr) {
    List<String> lines = Arrays.asList(tableStr.split("\n"));
    return lines.get(1).startsWith("|") ? lines.get(1) : "";
  }

  private static Map<String, Integer> countDataRows(String tableStr) {
    List<String> lines = Arrays.asList(tableStr.split("\n"));
    Map<String, Integer> counts = new HashMap<>();

    int start = -1, end = -1;

    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith("+") && start == -1) {
        start = i + 2;
      } else if (start != -1 && lines.get(i).startsWith("+")) {
        end = i;
        break;
      }
    }

    if (start == -1 || end == -1 || start >= end) {
      return Collections.emptyMap();
    }

    for (int i = start; i < end; i++) {
      String row = lines.get(i);
      counts.put(row, counts.getOrDefault(row, 0) + 1);
    }

    return counts;
  }
}
