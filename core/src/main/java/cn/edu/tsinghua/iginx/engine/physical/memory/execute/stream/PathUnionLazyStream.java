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

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.PathUnion;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PathUnionLazyStream extends BinaryLazyStream {

  private final PathUnion union;

  private boolean hasInitialized = false;

  private Header header;

  private Row nextA;

  private Row nextB;

  public PathUnionLazyStream(PathUnion union, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.union = union;
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    Header headerA = streamA.getHeader();
    Header headerB = streamB.getHeader();
    if (headerA.hasKey() ^ headerB.hasKey()) {
      throw new InvalidOperatorParameterException("row stream to be union must have same fields");
    }
    boolean hasTimestamp = headerA.hasKey();
    Set<Field> targetFieldSet = new HashSet<>();
    targetFieldSet.addAll(headerA.getFields());
    targetFieldSet.addAll(headerB.getFields());
    List<Field> targetFields = new ArrayList<>(targetFieldSet);
    if (hasTimestamp) {
      header = new Header(Field.KEY, targetFields);
    } else {
      header = new Header(targetFields);
    }
    hasInitialized = true;
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
    return nextA != null || nextB != null || streamA.hasNext() || streamB.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    if (!header.hasKey()) {
      // 不包含时间戳，只需要迭代式的顺次访问两个 stream 即可
      if (streamA.hasNext()) {
        return streamA.next();
      }
      return streamB.next();
    }
    if (nextA == null && streamA.hasNext()) {
      nextA = streamA.next();
    }
    if (nextB == null && streamB.hasNext()) {
      nextB = streamB.next();
    }
    if (nextA == null) { // 流 A 被消费完毕
      Row row = nextB;
      nextB = null;
      return RowUtils.transform(row, header);
    }
    if (nextB == null) { // 流 B 被消费完毕
      Row row = nextA;
      nextA = null;
      return RowUtils.transform(row, header);
    }
    if (nextA.getKey() == nextB.getKey()) {
      getContext().addWarningMessage("The query results contain overlapped keys.");
    }
    if (nextA.getKey() <= nextB.getKey()) {
      Row row = nextA;
      nextA = null;
      return RowUtils.transform(row, header);
    } else {
      Row row = nextB;
      nextB = null;
      return RowUtils.transform(row, header);
    }
  }
}
