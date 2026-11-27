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
package cn.edu.tsinghua.iginx.transform.data;

import static cn.edu.tsinghua.iginx.utils.TagKVUtils.fromFullName;
import static cn.edu.tsinghua.iginx.utils.TagKVUtils.tagMatchRegex;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PemjaReader extends AbstractBatchReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PemjaReader.class);

  private final List<Object> data;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  public PemjaReader(Object data, int batchSize) throws ReadBatchException {
    super(batchSize);
    if (!isList(data) || ((List<Object>) data).isEmpty()) {
      throw new ReadBatchException(
          "Python transformer must return a non-empty list starting with header, got: " + data);
    }
    this.data = (List<Object>) data;

    this.header = getHeaderFromData();
    this.rowList = getRowListFromData();
  }

  private Header getHeaderFromData() throws ReadBatchException {
    List<Field> fieldList = new ArrayList<>();
    if (!isList(data.get(0))) {
      throw new ReadBatchException(
          "Python transformer must return a 2D list whose first row is header, got: " + data);
    }
    List<Object> firstRow = (List<Object>) data.get(0);
    if (firstRow.isEmpty()) {
      throw new ReadBatchException("Header returned by python transformer has no column.");
    }
    checkRows(firstRow.size());
    List<DataType> typeList = parseTypeList(firstRow.size());

    String fieldName;
    Pair<String, Map<String, String>> p;
    for (int i = 0; i < firstRow.size(); i++) {
      if (!(firstRow.get(i) instanceof String) || ((String) firstRow.get(i)).isEmpty()) {
        throw new ReadBatchException(
            "Column name returned by python transformer must be a non-empty string, got: "
                + firstRow.get(i));
      }
      fieldName = (String) firstRow.get(i);
      Matcher matcher = tagMatchRegex.matcher(fieldName);
      if (!matcher.find()) {
        throw new ReadBatchException(
            "Invalid path :" + fieldName + ". Tags should be wrapped around by '{' & '}'.");
      } else {
        p = fromFullName(fieldName);
        fieldList.add(new Field(p.k, typeList.get(i), p.v));
      }
    }
    return new Header(fieldList);
  }

  private List<Row> getRowListFromData() {
    List<Row> rowList = new ArrayList<>();

    for (int i = 1; i < data.size(); i++) {
      rowList.add(new Row(header, ((List<Object>) data.get(i)).toArray()));
    }
    return rowList;
  }

  private boolean isList(Object object) {
    if (object == null) {
      return false;
    }
    return object instanceof List<?>;
  }

  private void checkRows(int columnCount) throws ReadBatchException {
    for (int i = 1; i < data.size(); i++) {
      if (!isList(data.get(i)) || ((List<Object>) data.get(i)).size() != columnCount) {
        throw new ReadBatchException(
            String.format(
                "Row %d returned by python transformer must be a list of %d values, got: %s",
                i, columnCount, data.get(i)));
      }
    }
  }

  // type of a column is decided by its first non-null value, BINARY if all values are null
  private List<DataType> parseTypeList(int columnCount) throws ReadBatchException {
    List<DataType> res = new ArrayList<>();
    for (int i = 0; i < columnCount; i++) {
      Object value = null;
      for (int j = 1; j < data.size() && value == null; j++) {
        value = ((List<Object>) data.get(j)).get(i);
      }
      // python won't pass integer, float, byte
      if (value instanceof Long) {
        res.add(DataType.LONG);
      } else if (value instanceof Double) {
        res.add(DataType.DOUBLE);
      } else if (value instanceof Boolean) {
        res.add(DataType.BOOLEAN);
      } else if (value == null || value instanceof String || value instanceof byte[]) {
        res.add(DataType.BINARY);
      } else {
        throw new ReadBatchException(
            "Invalid datatype for " + value + " of class: " + value.getClass());
      }
    }
    return res;
  }

  @Override
  protected Header readHeader() {
    return header;
  }

  @Override
  protected boolean hasNextRow() {
    return offset < rowList.size();
  }

  @Override
  protected Row nextRow() {
    return rowList.get(offset++);
  }

  @Override
  public void close() {
    rowList.clear();
  }
}
