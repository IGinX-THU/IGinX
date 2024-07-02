package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.calculateHashJoinPath;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.checkJoinColumns;
import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.getSamePathWithSpecificPrefix;
import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.getHash;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class HashInnerJoinLazyStream extends BinaryLazyStream {

  private final InnerJoin innerJoin;

  private final HashMap<Integer, List<Row>> streamBHashMap;

  private final Deque<Row> cache;

  private Header header;

  private int index;

  private boolean hasInitialized = false;

  private String joinPathA;

  private String joinPathB;

  private List<String> joinColumns;

  private List<String> extraJoinPaths;

  private boolean needTypeCast = false;

  public HashInnerJoinLazyStream(InnerJoin innerJoin, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.innerJoin = innerJoin;
    this.streamBHashMap = new HashMap<>();
    this.cache = new LinkedList<>();
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
    checkJoinColumns(joinColumns, headerA, headerB, innerJoin.getPrefixA(), innerJoin.getPrefixB());

    // 检查左右两表需要进行额外连接的path
    this.extraJoinPaths = new ArrayList<>();
    if (!innerJoin.getExtraJoinPrefix().isEmpty()) {
      this.extraJoinPaths =
          getSamePathWithSpecificPrefix(
              streamA.getHeader(), streamB.getHeader(), innerJoin.getExtraJoinPrefix());
    }
    // 计算建立和访问哈希表所用的path
    Pair<String, String> pair =
        calculateHashJoinPath(
            streamA.getHeader(),
            streamB.getHeader(),
            innerJoin.getPrefixA(),
            innerJoin.getPrefixB(),
            innerJoin.getFilter(),
            joinColumns,
            extraJoinPaths);
    this.joinPathA = pair.k;
    this.joinPathB = pair.v;

    this.index = headerB.indexOf(joinPathB);
    int indexA = headerA.indexOf(joinPathA);
    DataType dataTypeA = headerA.getField(indexA).getType();
    DataType dataTypeB = headerB.getField(index).getType();
    if (ValueUtils.isNumericType(dataTypeA) && ValueUtils.isNumericType(dataTypeB)) {
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
    }

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
    int hash = getHash(value, needTypeCast);

    if (streamBHashMap.containsKey(hash)) {
      for (Row rowB : streamBHashMap.get(hash)) {
        if (!RowUtils.equalOnSpecificPaths(rowA, rowB, extraJoinPaths)) {
          continue;
        } else if (!RowUtils.equalOnSpecificPaths(
            rowA, rowB, innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns)) {
          continue;
        }
        Row joinedRow =
            RowUtils.constructNewRow(
                header,
                rowA,
                rowB,
                innerJoin.getPrefixA(),
                innerJoin.getPrefixB(),
                true,
                joinColumns,
                extraJoinPaths);
        if (innerJoin.getFilter() != null) {
          if (!FilterUtils.validate(innerJoin.getFilter(), joinedRow)) {
            continue;
          }
        }
        cache.addLast(joinedRow);
      }
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
