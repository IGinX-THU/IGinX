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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

  private final List<Object> data;

  private final int batchSize;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  public PemjaReader(List<Object> data, int batchSize) throws ReadBatchException {
    this.data = data;
    this.batchSize = batchSize;

    this.header = getHeaderFromData();
    this.rowList = getRowListFromData();
  }

  private Header getHeaderFromData() throws ReadBatchException {
    List<Object> firstRow;
    List<DataType> typeList = new ArrayList<>();
    List<Field> fieldList = new ArrayList<>();
    if (isList(data.get(0))) {
      firstRow = (List<Object>) data.get(0);
      if (data.size() >= 2 && isList(data.get(1))) {
        typeList = parseTypeList((List<Object>) data.get(1));
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
        rowList.add(new Row(header, ((List<Object>) data.get(i)).toArray()));
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

  private List<DataType> parseTypeList(List<Object> dataList) {
    List<DataType> res = new ArrayList<>();
    for (Object value : dataList) {
      // python won't pass integer, float, byte
      if (value instanceof Long) {
        res.add(DataType.LONG);
      } else if (value instanceof Double) {
        res.add(DataType.DOUBLE);
      } else if (value instanceof Boolean) {
        res.add(DataType.BOOLEAN);
      } else if (value instanceof String || value instanceof byte[]) {
        res.add(DataType.BINARY);
      } else {
        throw new IllegalArgumentException(
            "Invalid datatype for " + value + " of class: " + value.getClass());
      }
    }
    return res;
  }

  @Override
  public boolean hasNextBatch() {
    return offset < rowList.size();
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
    return batchData;
  }

  @Override
  public void close() {
    rowList.clear();
  }
}
