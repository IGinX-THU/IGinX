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
package cn.edu.tsinghua.iginx.session;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getBytesFromValueByDataType;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExportCSV;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class QueryDataSet {

  enum State {
    HAS_MORE,
    NO_MORE,
    UNKNOWN
  }

  private final Session session;

  private final long queryId;

  private final int fetchSize;

  private ByteUtils.DataSet dataSet;

  private String exportStreamDir;

  private ExportCSV exportCSV;

  private State state;

  private int index;

  private String warningMsg;

  public QueryDataSet(
      Session session,
      long queryId,
      int fetchSize,
      ByteUtils.DataSet dataSet,
      String warningMsg,
      String exportStreamDir,
      ExportCSV exportCSV) {
    this.session = session;
    this.queryId = queryId;
    this.fetchSize = fetchSize;
    this.dataSet = dataSet;
    this.exportStreamDir = exportStreamDir;
    this.exportCSV = exportCSV;
    this.state = State.UNKNOWN;
    this.index = 0;
    this.warningMsg = warningMsg;
  }

  public void close() throws SessionException {
    session.closeQuery(queryId);
  }

  private void fetch() throws SessionException {
    if (dataSet != null && index != dataSet.getValues().size()) {
      return;
    }

    index = 0;

    Pair<List<ByteBuffer>, Boolean> pair = session.fetchResult(queryId, fetchSize);
    if (pair.k != null) {
      dataSet = ByteUtils.getDataFromArrowData(pair.k);
    }
    state = pair.v ? State.HAS_MORE : State.NO_MORE;
  }

  public boolean hasMore() throws SessionException {
    if (dataSet != null && index < dataSet.getValues().size()) {
      return true;
    }

    dataSet = null;
    index = 0;

    if (state == State.HAS_MORE || state == State.UNKNOWN) {
      fetch();
    }
    return dataSet != null;
  }

  public Object[] nextRow() throws SessionException {
    if (!hasMore()) {
      return null;
    }

    // nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
    if (dataSet.hasKey()) {
      List<Object> row = dataSet.getValues().get(index);
      Object[] result = new Object[1 + row.size()];
      result[0] = dataSet.getKeys()[index];
      System.arraycopy(row.toArray(), 0, result, 1, row.size());
      index++;
      return result;
    } else {
      return dataSet.getValues().get(index++).toArray();
    }
  }

  public List<byte[]> nextRowAsBytes() throws SessionException {
    if (!hasMore()) {
      return null;
    }
    int colSize = dataSet.getColSize();
    // nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
    List<byte[]> bytesValues = new ArrayList<>(colSize);
    List<Object> values = dataSet.getValues().get(index);
    index++;
    for (int i = 0; i < colSize; i++) {
      if (values.get(i) == null) {
        bytesValues.add(new byte[0]);
      } else {
        bytesValues.add(
            getBytesFromValueByDataType(values.get(i), dataSet.getDataTypeList().get(i)));
      }
    }
    return bytesValues;
  }

  public boolean hasKey() {
    return dataSet.hasKey();
  }

  public List<String> getColumnList() {
    return dataSet.getPaths();
  }

  public List<DataType> getDataTypeList() {
    return dataSet.getDataTypeList();
  }

  public String getWarningMsg() {
    return warningMsg;
  }

  public String getExportStreamDir() {
    return exportStreamDir;
  }

  public ExportCSV getExportCSV() {
    return exportCSV;
  }
}
