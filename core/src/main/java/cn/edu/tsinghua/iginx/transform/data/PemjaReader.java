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

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.transform.api.Reader;
import java.util.ArrayList;
import java.util.List;

public class PemjaReader implements Reader {

  private final List<Object> data;

  private final int batchSize;

  private final Header header;

  private final List<Row> rowList;

  private int offset = 0;

  public PemjaReader(List<Object> data, int batchSize) {
    this.data = data;
    this.batchSize = batchSize;

    this.header = getHeaderFromData();
    this.rowList = getRowListFromData();
  }

  private Header getHeaderFromData() {
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

    for (int i = 0; i < firstRow.size(); i++) {
      Object fieldName = firstRow.get(i);
      fieldList.add(
          new Field((String) fieldName, typeList.isEmpty() ? DataType.BINARY : typeList.get(i)));
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
      // python won't pass integer & float
      if (value instanceof Long) {
        res.add(DataType.LONG);
      } else if (value instanceof Double) {
        res.add(DataType.DOUBLE);
      } else if (value instanceof Boolean) {
        res.add(DataType.BOOLEAN);
      } else {
        res.add(DataType.BINARY);
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
