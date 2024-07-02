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

import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.ValueToSelectedPath;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;

public class ValueToSelectedPathLazyStream extends UnaryLazyStream {

  private final ValueToSelectedPath valueToSelectedPath;

  private final Deque<Row> cache;

  private String prefix;

  private boolean prefixIsEmpty;

  private int fieldSize;

  private Header header;

  private boolean hasInitialized = false;

  public ValueToSelectedPathLazyStream(ValueToSelectedPath valueToSelectedPath, RowStream stream) {
    super(stream);
    this.valueToSelectedPath = valueToSelectedPath;
    this.cache = new LinkedList<>();
  }

  private void initialize() throws PhysicalException {
    this.prefix = valueToSelectedPath.getPrefix();
    this.prefixIsEmpty = prefix.isEmpty();
    this.fieldSize = stream.getHeader().getFieldSize();
    this.header = new Header(Collections.singletonList(new Field("SelectedPath", DataType.BINARY)));
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
    while (cache.isEmpty() && (stream.hasNext())) {
      Row row = stream.next();
      for (int i = 0; i < fieldSize; i++) {
        String path =
            prefixIsEmpty
                ? row.getAsValue(i).getAsString()
                : prefix + DOT + row.getAsValue(i).getAsString();
        Object[] value = new Object[1];
        value[0] = path.getBytes(StandardCharsets.UTF_8);
        cache.addLast(new Row(header, value));
      }
    }
    return !cache.isEmpty();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return cache.pollFirst();
  }
}
