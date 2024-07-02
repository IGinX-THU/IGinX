package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import java.util.ArrayList;
import java.util.List;

public class NestedLoopInnerJoinLazyStream extends BinaryLazyStream {

  private final InnerJoin innerJoin;

  private final List<Row> streamBCache;

  private int[] indexOfJoinColumnInTableB;

  private List<String> joinColumns;

  private List<String> extraJoinPaths;

  private Header header;

  private int curStreamBIndex = 0;

  private boolean hasInitialized = false;

  private Row nextA;

  private Row nextB;

  private Row nextRow;

  public NestedLoopInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.innerJoin = innerJoin;
    this.streamBCache = new ArrayList<>();
  }

  private void initialize() throws PhysicalException {
    Header headerA = streamA.getHeader();
    Header headerB = streamB.getHeader();
    this.joinColumns = new ArrayList<>(innerJoin.getJoinColumns());

    // 计算自然连接的连接列名
    if (innerJoin.isNaturalJoin()) {
      RowUtils.fillNaturalJoinColumns(
          joinColumns, headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());
    }

    // 检查连接列名是否合法
    RowUtils.checkJoinColumns(
        joinColumns, headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    extraJoinPaths = new ArrayList<>();
    if (!innerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          RowUtils.getSamePathWithSpecificPrefix(headerA, headerB, innerJoin.getExtraJoinPrefix());
    }

    // 计算连接之后的header
    this.header =
        HeaderUtils.constructNewHead(
            headerA,
            headerB,
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            true,
            joinColumns,
            extraJoinPaths);

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
    if (nextRow != null) {
      return true;
    }
    while (nextRow == null && hasMoreRows()) {
      nextRow = tryMatch();
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
      return curStreamBIndex < streamBCache.size();
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
        nextA = streamA.next();
        curStreamBIndex = 0;
        nextB = streamBCache.get(curStreamBIndex);
      }
      curStreamBIndex++;
    }

    if (!RowUtils.equalOnSpecificPaths(nextA, nextB, extraJoinPaths)) {
      nextB = null;
      return null;
    } else if (!RowUtils.equalOnSpecificPaths(
        nextA, nextB, innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns)) {
      nextB = null;
      return null;
    }
    Row joinedRow =
        RowUtils.constructNewRow(
            header,
            nextA,
            nextB,
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            true,
            joinColumns,
            extraJoinPaths);
    if (innerJoin.getFilter() != null) {
      if (!FilterUtils.validate(innerJoin.getFilter(), joinedRow)) {
        nextB = null;
        return null;
      }
    }
    nextB = null;
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
