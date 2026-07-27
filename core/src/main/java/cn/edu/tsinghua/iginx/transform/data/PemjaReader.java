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
import cn.edu.tsinghua.iginx.transform.api.Reader;
import cn.edu.tsinghua.iginx.transform.exception.ReadBatchException;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PemjaReader implements Reader {

  private static final Logger LOGGER = LoggerFactory.getLogger(PemjaReader.class);

  private final List<?> data;

  private final int batchSize;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  private boolean hasLoadedEmptyBatch;

  public PemjaReader(Object result, int batchSize) throws ReadBatchException {
    if (!(result instanceof List<?>)) {
      throw new ReadBatchException("Python transformer must return a list of rows.");
    }
    this.data = (List<?>) result;
    this.batchSize = batchSize;

    validateData();
    this.header = getHeaderFromData();
    this.rowList = getRowListFromData();
  }

  private void validateData() throws ReadBatchException {
    if (data.isEmpty()) {
      throw new ReadBatchException("Python transformer returned an empty list without a header.");
    }
    if (!isList(data.get(0))) {
      throw new ReadBatchException(
          "The first row returned by Python transformer must be a header list.");
    }

    List<?> header = (List<?>) data.get(0);
    if (header.isEmpty()) {
      throw new ReadBatchException("Python transformer returned an empty header.");
    }
    for (Object fieldName : header) {
      if (!(fieldName instanceof String)) {
        throw new ReadBatchException("Python transformer header must contain only strings.");
      }
    }
    for (int i = 1; i < data.size(); i++) {
      if (!isList(data.get(i))) {
        throw new ReadBatchException("Python transformer data rows must be lists.");
      }
      if (((List<?>) data.get(i)).size() != header.size()) {
        throw new ReadBatchException(
            "Python transformer data row size does not match header size.");
      }
    }
  }

  private Header getHeaderFromData() throws ReadBatchException {
    List<?> firstRow;
    List<DataType> typeList = new ArrayList<>();
    List<Field> fieldList = new ArrayList<>();
    if (isList(data.get(0))) {
      firstRow = (List<?>) data.get(0);
      if (data.size() >= 2) {
        typeList = parseTypeList(firstRow.size());
      }
    } else {
      firstRow = data;
    }

    String fieldName;
    Pair<String, Map<String, String>> p;
    for (int i = 0; i < firstRow.size(); i++) {
      fieldName = (String) firstRow.get(i);
      Matcher matcher = tagMatchRegex.matcher(fieldName);
      if (!matcher.find()) {
        throw new ReadBatchException(
            "Invalid path :" + fieldName + ". Tags should be wrapped around by '{' & '}'.");
      } else {
        p = fromFullName(fieldName);
        fieldList.add(new Field(p.k, typeList.isEmpty() ? DataType.BINARY : typeList.get(i), p.v));
      }
    }
    return new Header(fieldList);
  }

  private List<Row> getRowListFromData() {
    List<Row> rowList = new ArrayList<>();

    boolean is2DList = isList(data.get(0));
    if (is2DList) {
      for (int i = 1; i < data.size(); i++) {
        rowList.add(new Row(header, ((List<?>) data.get(i)).toArray()));
      }
    }
    return rowList;
  }

  private boolean isList(Object object) {
    if (object == null) {
      return false;
    }
    return object instanceof List<?>;
  }

  private List<DataType> parseTypeList(int fieldSize) throws ReadBatchException {
    List<DataType> res = new ArrayList<>();
    for (int column = 0; column < fieldSize; column++) {
      DataType type = DataType.BINARY;
      boolean hasValue = false;
      for (int row = 1; row < data.size(); row++) {
        Object value = ((List<?>) data.get(row)).get(column);
        if (value == null) {
          continue;
        }
        DataType valueType = getDataType(value);
        if (hasValue && valueType != type) {
          throw new ReadBatchException(
              "Python transformer data column " + column + " contains inconsistent value types.");
        }
        type = valueType;
        hasValue = true;
      }
      res.add(type);
    }
    return res;
  }

  private DataType getDataType(Object value) throws ReadBatchException {
    // Python values are converted to Long, Double, Boolean, String, or byte[].
    if (value instanceof Long) {
      return DataType.LONG;
    } else if (value instanceof Double) {
      return DataType.DOUBLE;
    } else if (value instanceof Boolean) {
      return DataType.BOOLEAN;
    } else if (value instanceof String || value instanceof byte[]) {
      return DataType.BINARY;
    }
    throw new ReadBatchException(
        "Invalid datatype for " + value + " of class: " + value.getClass());
  }

  @Override
  public boolean hasNextBatch() {
    return offset < rowList.size() || (rowList.isEmpty() && !hasLoadedEmptyBatch);
  }

  @Override
  public BatchData loadNextBatch() {
    BatchData batchData = new BatchData(header);
    int countDown = batchSize;
    while (countDown > 0 && offset < rowList.size()) {
      batchData.appendRow(rowList.get(offset));
      countDown--;
      offset++;
    }
    if (batchData.isEmpty()) {
      hasLoadedEmptyBatch = true;
    }
    return batchData;
  }

  @Override
  public void close() {
    rowList.clear();
  }
}
