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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStreamWrapper;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import java.util.ArrayList;
import java.util.List;

public class DownsampleLazyStream extends UnaryLazyStream {

  private final RowStreamWrapper wrapper;

  private final Downsample downsample;

  private final List<FunctionCall> functionCallList;

  private Row nextTarget;

  private boolean hasInitialized = false;

  private Header header;

  public DownsampleLazyStream(Downsample downsample, RowStream stream) {
    super(stream);
    this.wrapper = new RowStreamWrapper(stream);
    this.downsample = downsample;
    this.functionCallList = downsample.getFunctionCallList();
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    nextTarget = loadNext();
    if (nextTarget != null) {
      header = nextTarget.getHeader();
    }
    hasInitialized = true;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (header == null) {
      header = Header.EMPTY_HEADER;
    }
    return header;
  }

  private Row loadNext() throws PhysicalException {
    if (nextTarget != null) {
      return nextTarget;
    }
    Row row = null;
    long timestamp = 0;
    long bias = downsample.getKeyRange().getActualBeginKey();
    long endKey = downsample.getKeyRange().getActualEndKey();
    long precision = downsample.getPrecision();
    long slideDistance = downsample.getSlideDistance();
    // startKey + (n - 1) * slideDistance + precision - 1 >= endKey
    int n = (int) (Math.ceil((double) (endKey - bias - precision + 1) / slideDistance) + 1);
    while (row == null && wrapper.hasNext()) {
      timestamp = wrapper.nextTimestamp() - (wrapper.nextTimestamp() - bias) % precision;
      List<Row> rows = new ArrayList<>();
      while (wrapper.hasNext() && wrapper.nextTimestamp() < timestamp + precision) {
        rows.add(wrapper.next());
      }
      Table table = new Table(rows.get(0).getHeader(), rows);

      List<Row> subRowList = new ArrayList<>();
      for (FunctionCall functionCall : functionCallList) {
        FunctionParams params = functionCall.getParams();
        SetMappingFunction function = (SetMappingFunction) functionCall.getFunction();
        try {
          subRowList.add(function.transform(table, params));
        } catch (Exception e) {
          throw new PhysicalTaskExecuteFailureException(
              "encounter error when execute set mapping function " + function.getIdentifier() + ".",
              e);
        }
      }
      row = RowUtils.combineMultipleColumns(subRowList);
    }
    return row == null
        ? null
        : new Row(new Header(Field.KEY, row.getHeader().getFields()), timestamp, row.getValues());
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (nextTarget == null) {
      nextTarget = loadNext();
    }
    return nextTarget != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextTarget;
    nextTarget = null;
    return row;
  }
}
