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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashSingleJoinLazyStream extends BinaryLazyStream {

  private final SingleJoin singleJoin;
  private final HashMap<Integer, List<Row>> streamBHashMap;
  private final Deque<Row> cache;
  private Header header;
  private boolean hasInitialized = false;
  private String joinPathA;
  private boolean needTypeCast = false;

  public HashSingleJoinLazyStream(SingleJoin singleJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.singleJoin = singleJoin;
    this.streamBHashMap = new HashMap<>();
    this.cache = new LinkedList<>();
  }

  private void initialize() throws PhysicalException {
    this.header = HeaderUtils.constructNewHead(streamA.getHeader(), streamB.getHeader(), true);
    Pair<String, String> joinPath =
        FilterUtils.getJoinPathFromFilter(
            singleJoin.getFilter(), streamA.getHeader(), streamB.getHeader());
    this.joinPathA = joinPath.k;
    String joinPathB = joinPath.v;

    DataType dataType1 =
        streamA.getHeader().getField(streamA.getHeader().indexOf(joinPathA)).getType();
    DataType dataType2 =
        streamB.getHeader().getField(streamB.getHeader().indexOf(joinPathB)).getType();
    if (ValueUtils.isNumericType(dataType1) && ValueUtils.isNumericType(dataType2)) {
      this.needTypeCast = true;
    }

    while (streamB.hasNext()) {
      Row rowB = streamB.next();
      Value value = rowB.getAsValue(joinPathB);
      if (value.isNull()) {
        continue;
      }
      if (needTypeCast) {
        value = ValueUtils.transformToDouble(value);
      }
      int hash;
      if (value.getDataType() == DataType.BINARY) {
        hash = Arrays.hashCode(value.getBinaryV());
      } else {
        hash = value.getValue().hashCode();
      }
      List<Row> rows = streamBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      rows.add(rowB);
    }

    this.hasInitialized = true;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    while (cache.isEmpty() && streamA.hasNext()) {
      tryMatch();
    }
    return !cache.isEmpty();
  }

  private void tryMatch() throws PhysicalException {
    Row rowA = streamA.next();

    Value value = rowA.getAsValue(joinPathA);
    if (value.isNull()) {
      return;
    }
    if (needTypeCast) {
      value = ValueUtils.transformToDouble(value);
    }
    int hash;
    if (value.getDataType() == DataType.BINARY) {
      hash = Arrays.hashCode(value.getBinaryV());
    } else {
      hash = value.getValue().hashCode();
    }

    if (streamBHashMap.containsKey(hash)) {
      List<Row> hashRowsB = streamBHashMap.get(hash);
      if (hashRowsB.size() == 1) {
        Row joinedRow = RowUtils.constructNewRow(header, rowA, hashRowsB.get(0), true);
        cache.add(joinedRow);
      } else {
        throw new PhysicalException("the return value of sub-query has more than one rows");
      }
    } else {
      int anotherRowSize = streamB.getHeader().getFieldSize();
      Row unmatchedRow =
          RowUtils.constructUnmatchedRow(
              header, rowA, singleJoin.getPrefixA(), anotherRowSize, true);
      cache.add(unmatchedRow);
    }
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return cache.pollFirst();
  }
}
