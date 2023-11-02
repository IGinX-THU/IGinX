/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.session;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getBytesFromByteBufferByDataType;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExportCSV;
import cn.edu.tsinghua.iginx.thrift.QueryDataSetV2;
import cn.edu.tsinghua.iginx.utils.Bitmap;
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

  private final List<String> columnList;

  private final List<DataType> dataTypeList;

  private final int fetchSize;

  private List<ByteBuffer> valuesList;

  private List<ByteBuffer> bitmapList;

  private String exportStreamDir;

  private ExportCSV exportCSV;

  private State state;

  private int index;

  private String warningMsg;

  public QueryDataSet(
      Session session,
      long queryId,
      List<String> columnList,
      List<DataType> dataTypeList,
      int fetchSize,
      List<ByteBuffer> valuesList,
      List<ByteBuffer> bitmapList,
      String warningMsg,
      String exportStreamDir,
      ExportCSV exportCSV) {
    this.session = session;
    this.queryId = queryId;
    this.columnList = columnList;
    this.dataTypeList = dataTypeList;
    this.fetchSize = fetchSize;
    this.valuesList = valuesList;
    this.bitmapList = bitmapList;
    this.exportStreamDir = exportStreamDir;
    this.exportCSV = exportCSV;
    this.state = State.UNKNOWN;
    this.index = 0;
    this.warningMsg = warningMsg;
  }

  public void close() throws SessionException, ExecutionException {
    session.closeQuery(queryId);
  }

  private void fetch() throws SessionException, ExecutionException {
    if (bitmapList != null && index != bitmapList.size()) { // 只有之前的被消费完才有可能继续取数据
      return;
    }
    bitmapList = null;
    valuesList = null;
    index = 0;

    Pair<QueryDataSetV2, Boolean> pair = session.fetchResult(queryId, fetchSize);
    if (pair.k != null) {
      bitmapList = pair.k.bitmapList;
      valuesList = pair.k.valuesList;
    }
    state = pair.v ? State.HAS_MORE : State.NO_MORE;
  }

  public boolean hasMore() throws SessionException, ExecutionException {
    if (valuesList != null && index < valuesList.size()) {
      return true;
    }
    bitmapList = null;
    valuesList = null;
    index = 0;
    if (state == State.HAS_MORE || state == State.UNKNOWN) {
      fetch();
    }
    return valuesList != null;
  }

  public Object[] nextRow() throws SessionException, ExecutionException {
    if (!hasMore()) {
      return null;
    }
    // nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
    ByteBuffer valuesBuffer = valuesList.get(index);
    ByteBuffer bitmapBuffer = bitmapList.get(index);
    index++;
    Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
    Object[] values = new Object[dataTypeList.size()];
    for (int i = 0; i < dataTypeList.size(); i++) {
      if (bitmap.get(i)) {
        values[i] = getValueFromByteBufferByDataType(valuesBuffer, dataTypeList.get(i));
      }
    }
    return values;
  }

  public List<byte[]> nextRowAsBytes() throws SessionException, ExecutionException {
    if (!hasMore()) {
      return null;
    }
    // nextRow 只会返回本地的 row，如果本地没有，在进行 hasMore 操作时候，就一定也已经取回来了
    ByteBuffer valuesBuffer = valuesList.get(index);
    ByteBuffer bitmapBuffer = bitmapList.get(index);
    index++;
    Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
    List<byte[]> bytesValues = new ArrayList<>(dataTypeList.size());
    for (int i = 0; i < dataTypeList.size(); i++) {
      if (bitmap.get(i)) {
        bytesValues.add(getBytesFromByteBufferByDataType(valuesBuffer, dataTypeList.get(i)));
      } else {
        bytesValues.add(new byte[0]);
      }
    }
    return bytesValues;
  }

  public List<String> getColumnList() {
    return columnList;
  }

  public List<DataType> getDataTypeList() {
    return dataTypeList;
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
