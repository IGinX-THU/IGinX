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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataUtils.class);

  public static List<List<Object>> dataFromTable(Table table, List<String> paths) {
    List<Object> colNames = new ArrayList<>(Collections.singletonList("key"));
    List<Object> colTypes = new ArrayList<>(Collections.singletonList(DataType.LONG.toString()));
    List<Integer> indices = new ArrayList<>();

    for (String target : paths) {
      boolean found = false;
      if (StringUtils.isPattern(target)) {
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
        for (int i = 0; i < table.getHeader().getFieldSize(); i++) {
          Field field = table.getHeader().getField(i);
          if (pattern.matcher(field.getName()).matches()) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
            found = true;
          }
        }
      } else {
        for (int i = 0; i < table.getHeader().getFieldSize(); i++) {
          Field field = table.getHeader().getField(i);
          if (target.equals(field.getName())) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
            found = true;
            break;
          }
        }
      }
      if (!found) {
        LOGGER.warn("Cannot find data for path:{}", target);
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

    for (String target : paths) {
      boolean found = false;
      if (StringUtils.isPattern(target)) {
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
          Field field = row.getHeader().getField(i);
          if (pattern.matcher(field.getName()).matches()) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            rowData.add(row.getValues()[i]);
            found = true;
          }
        }
      } else {
        for (int i = 0; i < row.getHeader().getFieldSize(); i++) {
          Field field = row.getHeader().getField(i);
          if (target.equals(field.getName())) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            rowData.add(row.getValues()[i]);
            found = true;
            break;
          }
        }
      }
      if (!found) {
        LOGGER.warn("Cannot find data for path:{}", target);
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
