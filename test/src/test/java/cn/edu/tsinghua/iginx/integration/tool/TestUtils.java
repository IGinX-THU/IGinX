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
package cn.edu.tsinghua.iginx.integration.tool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
}
