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
package cn.edu.tsinghua.iginx.engine.shared.function.udf.utils;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public class DataUtils {

  public static List<List<Object>> dataFromTable(Table table, List<String> paths) {
    List<Object> colNames = new ArrayList<>(Collections.singletonList("key"));
    List<Object> colTypes = new ArrayList<>(Collections.singletonList(DataType.LONG.toString()));
    List<Integer> indices = new ArrayList<>();

    flag:
    for (String target : paths) {
      if (StringUtils.isPattern(target)) {
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
        for (int i = 0; i < table.getHeader().getFieldSize(); i++) {
          Field field = table.getHeader().getField(i);
          if (pattern.matcher(field.getName()).matches()) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
          }
        }
      } else {
        for (int i = 0; i < table.getHeader().getFieldSize(); i++) {
          Field field = table.getHeader().getField(i);
          if (target.equals(field.getName())) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
            continue flag;
          }
        }
      }
    }

    if (colNames.size() == 1) {
      return null;
    }

    List<List<Object>> data = new ArrayList<>();
    data.add(colNames);
    data.add(colTypes);
    for (Row row : table.getRows()) {
      List<Object> rowData = new ArrayList<>();
      rowData.add(row.getKey());
      for (Integer idx : indices) {
        rowData.add(row.getValues()[idx]);
      }
      data.add(rowData);
    }
    return data;
  }

  public static List<List<Object>> dataFromRow(Row row, List<String> paths) {
    List<Object> colNames = new ArrayList<>(Collections.singletonList("key"));
    List<Object> colTypes = new ArrayList<>(Collections.singletonList(DataType.LONG.toString()));
    List<Object> rowData = new ArrayList<>(Collections.singletonList(row.getKey()));

    flag:
    for (String target : paths) {
      if (StringUtils.isPattern(target)) {
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
          Field field = row.getHeader().getField(i);
          if (pattern.matcher(field.getName()).matches()) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            rowData.add(row.getValues()[i]);
          }
        }
      } else {
        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
          Field field = row.getHeader().getField(i);
          if (target.equals(field.getName())) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            rowData.add(row.getValues()[i]);
            continue flag;
          }
        }
      }
    }

    if (colNames.size() == 1) {
      return null;
    }

    List<List<Object>> data = new ArrayList<>();
    data.add(colNames);
    data.add(colTypes);
    data.add(rowData);

    return data;
  }
}
