package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.calculateHashJoinPath;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.constructNewHead;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.checkJoinColumns;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.equalOnSpecificPaths;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.getSamePathWithSpecificPrefix;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.getHash;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class HashOuterJoinLazyStream extends BinaryLazyStream {

  private final OuterJoin outerJoin;

  private final HashMap<Integer, List<Row>> streamBHashMap;

  private final List<Integer> streamBHashPutOrder;

  private final List<Row> unmatchedStreamARows; // 未被匹配过的StreamA的行

  private final Set<Integer> matchedStreamBRowHashSet; // 已被匹配过的StreamB的行

  private final Deque<Row> cache;

  private List<String> joinColumns;

  private List<String> extraJoinPaths;

  private boolean cutRight;

  private Header header;

  private int index;

  private boolean hasInitialized = false;

  private boolean lastPartHasInitialized = false; // 外连接未匹配部分是否被初始化

  private String joinPathA;

  private String joinPathB;

  private boolean needTypeCast = false;

  public HashOuterJoinLazyStream(OuterJoin outerJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.outerJoin = outerJoin;
    this.streamBHashMap = new HashMap<>();
    this.streamBHashPutOrder = new LinkedList<>();
    this.unmatchedStreamARows = new ArrayList<>();
    this.matchedStreamBRowHashSet = new HashSet<>();
    this.cache = new LinkedList<>();
  }

  private void initialize() throws PhysicalException {
    OuterJoinType outerJoinType = outerJoin.getOuterJoinType();
    Header headerA = streamA.getHeader();
    Header headerB = streamB.getHeader();
    this.joinColumns = new ArrayList<>(outerJoin.getJoinColumns());

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
    checkJoinColumns(joinColumns, headerA, headerB, outerJoin.getPrefixA(), outerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    this.extraJoinPaths = new ArrayList<>();
    if (!outerJoin.getExtraJoinPrefix().isEmpty()) {
      extraJoinPaths =
          getSamePathWithSpecificPrefix(
              streamA.getHeader(), streamB.getHeader(), outerJoin.getExtraJoinPrefix());
    }

    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            streamA.getHeader(),
            streamB.getHeader(),
            outerJoin.getPrefixA(),
            outerJoin.getPrefixB(),
            outerJoin.getFilter(),
            joinColumns,
            extraJoinPaths);
    this.joinPathA = pair.k;
    this.joinPathB = pair.v;

    this.cutRight = !outerJoin.getOuterJoinType().equals(OuterJoinType.RIGHT);

    int indexAnother;
    if (outerJoinType == OuterJoinType.RIGHT) {
      this.index = headerA.indexOf(joinPathA);
      indexAnother = headerB.indexOf(joinPathB);
    } else {
      this.index = headerB.indexOf(joinPathB);
      indexAnother = headerA.indexOf(joinPathA);
    }

    DataType dataType1 = headerA.getField(indexAnother).getType();
    DataType dataType2 = headerB.getField(index).getType();
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
      int hash = getHash(value, needTypeCast);
      List<Row> rows = streamBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      rows.add(rowB);
      if (rows.size() == 1) {
        streamBHashPutOrder.add(hash);
      }
    }

    // 计算连接之后的header
    this.header =
        constructNewHead(
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
        cache.add(unmatchedRow);
      }
    }
    if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
      int anotherRowSize =
          streamA.getHeader().hasKey() && outerJoin.getPrefixA() != null
              ? streamA.getHeader().getFieldSize() + 1
              : streamA.getHeader().getFieldSize();
      anotherRowSize -= joinColumns.size();
      anotherRowSize -= extraJoinPaths.size();

      for (int hash : streamBHashPutOrder) {
        if (!matchedStreamBRowHashSet.contains(hash)) {
          List<Row> unmatchedRows = streamBHashMap.get(hash);
          for (Row halfRow : unmatchedRows) {
            Row unmatchedRow =
                RowUtils.constructUnmatchedRow(
                    header, halfRow, outerJoin.getPrefixB(), anotherRowSize, false);
            cache.add(unmatchedRow);
          }
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
    if (!hasInitialized) {
      initialize();
    }
    while (cache.isEmpty() && streamA.hasNext()) {
      tryMatch();
    }
    if (cache.isEmpty() && !lastPartHasInitialized) {
      initializeLastPart();
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
    int hash = getHash(value, needTypeCast);

    if (streamBHashMap.containsKey(hash)) {
      List<Row> rowsB = streamBHashMap.get(hash);
      for (Row rowB : rowsB) {
        if (!equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
          continue;
        } else if (!equalOnSpecificPaths(
            rowA, rowB, outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumns)) {
          continue;
        }
        Row joinedRow =
            RowUtils.constructNewRow(
                header,
                rowA,
                rowB,
                outerJoin.getPrefixA(),
                outerJoin.getPrefixB(),
                cutRight,
                joinColumns,
                extraJoinPaths);
        if (outerJoin.getFilter() != null) {
          if (!FilterUtils.validate(outerJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        cache.addLast(joinedRow);
      }
      matchedStreamBRowHashSet.add(hash);
    } else {
      unmatchedStreamARows.add(rowA);
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
