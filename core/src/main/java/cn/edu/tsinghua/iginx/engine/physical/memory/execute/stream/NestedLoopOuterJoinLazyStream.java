package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NestedLoopOuterJoinLazyStream extends BinaryLazyStream {

  private final OuterJoin outerJoin;

  private final List<Row> streamBCache;

  private final List<Row> unmatchedStreamARows; // 未被匹配过的StreamA的行

  private final Set<Integer> matchedStreamBRowIndexSet; // 已被匹配过的StreamB的行

  private final List<Row> lastPart; // 后面多出未被匹配的结果行

  private List<String> joinColumns;

  private List<String> extraJoinPaths;

  private boolean cutRight;

  private int[] indexOfJoinColumnsInTable;

  private Header header;

  private boolean curNextAHasMatched = false; // 当前streamA的Row是否已被匹配过

  private int curStreamBIndex = 0;

  private boolean hasInitialized = false;

  private boolean lastPartHasInitialized = false; // 外连接未匹配部分是否被初始化

  private int lastPartIndex = 0;

  private Row nextA;

  private Row nextB;

  private Row nextRow;

  public NestedLoopOuterJoinLazyStream(OuterJoin outerJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.outerJoin = outerJoin;
    this.streamBCache = new ArrayList<>();
    this.unmatchedStreamARows = new ArrayList<>();
    this.matchedStreamBRowIndexSet = new HashSet<>();
    this.lastPart = new ArrayList<>();
  }

  private void initialize() throws PhysicalException {
    Header headerA = streamA.getHeader();
    Header headerB = streamB.getHeader();
    joinColumns = new ArrayList<>(outerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (outerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns,
          streamA.getHeader(),
          streamB.getHeader(),
          outerJoin.getPrefixA(),
          outerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    RowUtils.checkJoinColumns(
        joinColumns, headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    this.extraJoinPaths = new ArrayList<>();
    if (!outerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          RowUtils.getSamePathWithSpecificPrefix(
              streamA.getHeader(), streamB.getHeader(), outerJoin.getExtraJoinPrefix());
    }

    this.cutRight = !outerJoin.getOuterJoinType().equals(OuterJoinType.RIGHT);
    // 计算连接之后的header
    this.header =
        HeaderUtils.constructNewHead(
            streamA.getHeader(),
            streamB.getHeader(),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            cutRight,
            joinColumns,
            extraJoinPaths);

    this.hasInitialized = true;
  }

  private void initializeLastPart() throws PhysicalException {
    if (lastPartHasInitialized) {
      return;
    }
    OuterJoinType outerType = outerJoin.getOuterJoinType();
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
      int anotherRowSize =
          streamB.getHeader().hasKey() && outerJoin.getPrefixB() != null
              ? streamB.getHeader().getFieldSize() + 1
              : streamB.getHeader().getFieldSize();
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (Row halfRow : unmatchedStreamARows) {
        Row unmatchedRow =
            RowUtils.constructUnmatchedRow(
                header, halfRow, outerJoin.getPrefixA(), anotherRowSize, true);
        lastPart.add(unmatchedRow);
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
      int anotherRowSize =
          streamA.getHeader().hasKey() && outerJoin.getPrefixA() != null
              ? streamA.getHeader().getFieldSize() + 1
              : streamA.getHeader().getFieldSize();
      if (outerJoin.getFilter() == null) {
        anotherRowSize -= joinColumns.size();
      }
      for (int i = 0; i < streamBCache.size(); i++) {
        if (!matchedStreamBRowIndexSet.contains(i)) {
          Row unmatchedRow =
              RowUtils.constructUnmatchedRow(
                  header, streamBCache.get(i), outerJoin.getPrefixB(), anotherRowSize, false);
          lastPart.add(unmatchedRow);
        }
      }
    }
    this.lastPartHasInitialized = true;
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
      } else { // streamB和streamA中的一行全部匹配过了一遍
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

    if (!RowUtils.equalOnSpecificPaths(nextA, nextB, extraJoinPaths)) {
      nextB = null;
      return null;
    } else if (!RowUtils.equalOnSpecificPaths(
        nextA, nextB, outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumns)) {
      nextB = null;
      return null;
    }
    Row joinedRow =
        RowUtils.constructNewRow(
            header,
            nextA,
            nextB,
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            cutRight,
            joinColumns,
            extraJoinPaths);
    if (outerJoin.getFilter() != null) {
      if (!FilterUtils.validate(outerJoin.getFilter(), joinedRow)) {
        nextB = null;
        return null;
      }
    }
    nextB = null;
    this.curNextAHasMatched = true;
    this.matchedStreamBRowIndexSet.add(curStreamBIndex - 1);
    return joinedRow;
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
