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

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.checkHeadersComparable;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.isValueEqualRow;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.getHash;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class UnionDistinctLazyStream extends BinaryLazyStream {

  private final HashMap<Integer, List<Row>> hashMap;

  private final Deque<Row> cache;

  private Header header;

  private boolean hasKey;

  private boolean needTypeCast;

  private boolean hasInitialized = false;

  public UnionDistinctLazyStream(RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.hashMap = new HashMap<>();
    this.cache = new LinkedList<>();
  }

  private void initialize() throws PhysicalException {
    // 检查输入两表的header是否可比较
    checkHeadersComparable(streamA.getHeader(), streamB.getHeader());

    if (streamA.getHeader().getFields().isEmpty() || streamB.getHeader().getFields().isEmpty()) {
      throw new InvalidOperatorParameterException(
          "row stream to be union must have non-empty fields");
    }

    // 检查是否需要类型转换
    DataType dataTypeA = streamA.getHeader().getField(0).getType();
    DataType dataTypeB = streamB.getHeader().getField(0).getType();
    if (ValueUtils.isNumericType(dataTypeA) && ValueUtils.isNumericType(dataTypeB)) {
      this.needTypeCast = true;
    }

    this.header = streamA.getHeader();
    this.hasKey = header.hasKey();
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
    while (cache.isEmpty() && (streamA.hasNext() || streamB.hasNext())) {
      tryMatch();
    }
    return !cache.isEmpty();
  }

  private void tryMatch() throws PhysicalException {
    Row row;
    if (streamA.hasNext()) {
      row = streamA.next();
    } else if (streamB.hasNext()) {
      row = streamB.next();
    } else {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    int hash;
    if (hasKey) {
      hash = Objects.hash(row.getKey());
    } else {
      Value value = row.getAsValue(0);
      if (value.isNull()) {
        return;
      }
      hash = getHash(value, needTypeCast);
    }

    List<Row> rowsExist = hashMap.computeIfAbsent(hash, k -> new ArrayList<>());
    // 去重
    for (Row rowExist : rowsExist) {
      if (isValueEqualRow(rowExist, row, hasKey)) {
        return;
      }
    }

    Row targetRow;
    if (hasKey) {
      targetRow = new Row(header, row.getKey(), row.getValues());
    } else {
      targetRow = new Row(header, row.getValues());
    }
    rowsExist.add(targetRow);
    cache.addLast(targetRow);
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return cache.pollFirst();
  }
}
