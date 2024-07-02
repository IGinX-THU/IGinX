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
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopMarkJoinLazyStream extends BinaryLazyStream {

  private final MarkJoin markJoin;
  private final List<Row> streamBCache;
  private final List<Row> unmatchedStreamARows;
  private final List<Row> lastPart;
  private Header targetHeader;
  private Header joinHeader;
  private boolean curNextAHasMatched = false;
  private int curStreamBIndex = 0;
  private boolean hasInitialized = false;
  private boolean lastPartHasInitialized = false;
  private int lastPartIndex = 0;
  private Row nextA;
  private Row nextB;
  private Row nextRow;

  public NestedLoopMarkJoinLazyStream(MarkJoin markJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.markJoin = markJoin;
    this.streamBCache = new ArrayList<>();
    this.unmatchedStreamARows = new ArrayList<>();
    this.lastPart = new ArrayList<>();
  }

  private void initialize() throws PhysicalException {
    if (hasInitialized) {
      return;
    }
    this.targetHeader = HeaderUtils.constructNewHead(streamA.getHeader(), markJoin.getMarkColumn());
    this.joinHeader = HeaderUtils.constructNewHead(streamA.getHeader(), streamB.getHeader(), true);
    this.hasInitialized = true;
  }

  private void initializeLastPart() {
    if (lastPartHasInitialized) {
      return;
    }
    for (Row unmatchedRowA : unmatchedStreamARows) {
      Row unmatchedRow =
          RowUtils.constructNewRowWithMark(targetHeader, unmatchedRowA, markJoin.isAntiJoin());
      lastPart.add(unmatchedRow);
    }
    this.lastPartHasInitialized = true;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    return targetHeader;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (nextRow != null) {
      return true;
    }
    while (nextRow == null && hasMoreRows()) {
      nextRow = tryMatch();
    }
    if (nextRow == null) {
      initializeLastPart();
      if (lastPartIndex < lastPart.size()) {
        nextRow = lastPart.get(lastPartIndex);
        lastPartIndex++;
      }
    }
    return nextRow != null;
  }

  private boolean hasMoreRows() throws PhysicalException {
    if (!hasInitialized) {
      initialize();
    }
    if (streamA.hasNext()) {
      return true;
    } else {
      if (curStreamBIndex < streamBCache.size()) {
        return true;
      } else {
        if (nextA != null && !curNextAHasMatched) {
          unmatchedStreamARows.add(nextA);
          nextA = null;
        }
        return false;
      }
    }
  }

  private Row tryMatch() throws PhysicalException {
    if (!hasMoreRows()) {
      return null;
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
      } else {
        if (!curNextAHasMatched) {
          unmatchedStreamARows.add(nextA);
        }
        nextA = streamA.next();
        curNextAHasMatched = false;
        curStreamBIndex = 0;
        nextB = streamBCache.get(curStreamBIndex);
      }
      curStreamBIndex++;
    }

    Row joinedRow = RowUtils.constructNewRow(joinHeader, nextA, nextB, true);
    nextB = null;
    if (FilterUtils.validate(markJoin.getFilter(), joinedRow)) {
      if (!this.curNextAHasMatched) {
        this.curNextAHasMatched = true;
        return RowUtils.constructNewRowWithMark(targetHeader, nextA, !markJoin.isAntiJoin());
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row ret = nextRow;
    nextRow = null;
    return ret;
  }
}
