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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import java.util.ArrayList;
import java.util.List;

public class CrossJoinLazyStream extends BinaryLazyStream {

  private final CrossJoin crossJoin;

  private final List<Row> streamBCache;

  private Header header;

  private int curStreamBIndex = 0;

  private boolean hasInitialized = false;

  private Row nextA;

  private Row nextB;

  public CrossJoinLazyStream(CrossJoin crossJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.crossJoin = crossJoin;
    this.streamBCache = new ArrayList<>();
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    this.header =
        HeaderUtils.constructNewHead(
            streamA.getHeader(),
            streamB.getHeader(),
            crossJoin.getPrefixA(),
            crossJoin.getPrefixB());
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
    if (streamA.hasNext()) {
      return true;
    } else {
      return curStreamBIndex < streamBCache.size();
    }
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    if (nextA == null && streamA.hasNext()) {
      nextA = streamA.next();
    }
    if (nextB == null) {
      if (streamB.hasNext()) {
        nextB = streamB.next();
        streamBCache.add(nextB);
      } else if (curStreamBIndex < streamBCache.size()) {
        nextB = streamBCache.get(curStreamBIndex);
      } else { // streamB和streamA中的一行全部匹配过了一遍
        nextA = streamA.next();
        curStreamBIndex = 0;
        nextB = streamBCache.get(curStreamBIndex);
      }
      curStreamBIndex++;
    }

    Row nextRow =
        RowUtils.constructNewRow(
            header, nextA, nextB, crossJoin.getPrefixA(), crossJoin.getPrefixB());
    nextB = null;
    return nextRow;
  }
}
