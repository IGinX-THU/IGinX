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
import cn.edu.tsinghua.iginx.engine.shared.operator.Intersect;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

public class IntersectLazyStream extends BinaryLazyStream {

  private final Intersect intersect;

  private final HashMap<Integer, List<Row>> targetHashMap;

  private final HashMap<Integer, List<Row>> rowsBHashMap;

  private final Deque<Row> cache;

  private Header header;

  private boolean isDistinct;

  private boolean hasKey;

  private boolean needTypeCast;

  private boolean hasInitialized = false;

  public IntersectLazyStream(Intersect intersect, RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.intersect = intersect;
    this.targetHashMap = new HashMap<>();
    this.rowsBHashMap = new HashMap<>();
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
    this.isDistinct = intersect.isDistinct();
    this.hasKey = header.hasKey();

    // 扫描右表建立哈希表
    int hash;
    while (streamB.hasNext()) {
      Row rowB = streamB.next();
      if (hasKey) {
        hash = Objects.hash(rowB.getKey());
      } else {
        Value value = rowB.getAsValue(0);
        if (value == null) {
          continue;
        }
        hash = getHash(value, needTypeCast);
      }
      List<Row> rowsBExist = rowsBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
      rowsBExist.add(rowB);
    }

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

    int hash;
    if (hasKey) {
      hash = Objects.hash(rowA.getKey());
    } else {
      Value value = rowA.getAsValue(0);
      if (value == null) {
        return;
      }
      hash = getHash(value, needTypeCast);
    }

    List<Row> rowsB = rowsBHashMap.computeIfAbsent(hash, k -> new ArrayList<>());
    List<Row> rowsExist = targetHashMap.computeIfAbsent(hash, k -> new ArrayList<>());

    // 去重
    if (isDistinct) {
      for (Row rowExist : rowsExist) {
        if (isValueEqualRow(rowA, rowExist, hasKey)) {
          return;
        }
      }
    }

    // 保留左表和右表的公共部分
    for (Row rowB : rowsB) {
      if (isValueEqualRow(rowA, rowB, hasKey)) {
        rowsExist.add(rowA);
        cache.addLast(rowA);
        return;
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
