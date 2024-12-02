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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.AddSequence;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;

public class AddSequenceLazyStream extends UnaryLazyStream {

  private Header header;

  private final AddSequence addSequence;

  private final List<Long> cur;

  private final List<Long> increments;

  private final int oldSize;

  private final int newSize;

  private final int sequenceSize;

  public AddSequenceLazyStream(AddSequence addSequence, RowStream stream) throws PhysicalException {
    super(stream);
    this.addSequence = addSequence;
    this.cur = new ArrayList<>(addSequence.getStartList());
    this.increments = new ArrayList<>(addSequence.getIncrementList());
    this.header = getHeader();
    this.oldSize = stream.getHeader().getFieldSize();
    this.newSize = header.getFieldSize();
    this.sequenceSize = newSize - oldSize;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (header == null) {
      Header header = stream.getHeader();
      List<Field> targetFields = new ArrayList<>(stream.getHeader().getFields());
      addSequence
          .getColumns()
          .forEach(column -> targetFields.add(new Field(column, DataType.LONG)));
      this.header = new Header(header.getKey(), targetFields);
    }
    return header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return stream.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    Row row = stream.next();
    Object[] values = new Object[newSize];
    System.arraycopy(row.getValues(), 0, values, 0, oldSize);
    for (int i = 0; i < sequenceSize; i++) {
      values[oldSize + i] = cur.get(i);
      cur.set(i, cur.get(i) + increments.get(i));
    }
    return new Row(header, row.getKey(), values);
  }
}
