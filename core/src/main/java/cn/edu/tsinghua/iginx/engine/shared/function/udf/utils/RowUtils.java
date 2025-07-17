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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.DataTypeUtils;
import cn.edu.tsinghua.iginx.utils.TypeConverter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RowUtils {

  public static Header constructHeaderWithFirstTwoRowsUsingFuncName(
      List<List<Object>> res, boolean hasKey, String funcName) {
    List<Field> targetFields = new ArrayList<>();
    for (int i = 0; i < res.get(0).size(); i++) {
      String resColumnName = (String) res.get(0).get(i);
      if (resColumnName.matches(".*[(].*[)]")) {
        String resFuncName = resColumnName.substring(0, resColumnName.indexOf("("));
        resColumnName = resColumnName.replaceFirst(resFuncName, funcName);
      }
      targetFields.add(
          new Field(
              resColumnName, DataTypeUtils.getDataTypeFromString((String) res.get(1).get(i))));
    }
    return hasKey ? new Header(Field.KEY, targetFields) : new Header(targetFields);
  }

  public static Row constructNewRowWithKey(Header header, long key, List<Object> values) {
    Object[] rowValues = TypeConverter.convertToTypes(header.getDataTypes(), values);
    return new Row(header, key, rowValues);
  }

  public static Row constructNewRow(Header header, List<Object> values) {
    return constructNewRowWithKey(header, Row.NON_EXISTED_KEY, values);
  }

  public static Table constructNewTable(Header header, List<List<Object>> values, int startIndex) {
    List<Row> rowList =
        values.stream()
            .skip(startIndex)
            .map(row -> constructNewRow(header, row))
            .collect(Collectors.toList());
    return new Table(header, rowList);
  }

  public static Table constructNewTableWithKey(
      Header header, List<List<Object>> values, int startIndex) {
    List<Row> rowList = new ArrayList<>();
    Long key;
    for (int i = startIndex; i < values.size(); i++) {
      key = (Long) TypeConverter.convertToType(DataType.LONG, values.get(i).remove(0));
      rowList.add(constructNewRowWithKey(header, key, values.get(i)));
    }
    return new Table(header, rowList);
  }
}
