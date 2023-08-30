package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.HeaderUtils.checkHeadersComparable;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.Deque;
import java.util.LinkedList;

public class UnionAllLazyStream extends BinaryLazyStream {

  private final Deque<Row> cache;

  private Header header;

  private boolean hasKey;

  private boolean hasInitialized = false;

  public UnionAllLazyStream(RowStream streamA, RowStream streamB) {
    super(streamA, streamB);
    this.cache = new LinkedList<>();
  }

  private void initialize() throws PhysicalException {
    // 检查输入两表的header是否可比较
    checkHeadersComparable(streamA.getHeader(), streamB.getHeader());

    if (streamA.getHeader().getFields().isEmpty() || streamB.getHeader().getFields().isEmpty()) {
      throw new InvalidOperatorParameterException(
          "row stream to be union must have non-empty fields");
    }

    this.header = streamA.getHeader();
    this.hasKey = header.hasKey();
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
    while (cache.isEmpty() && (streamA.hasNext() || streamB.hasNext())) {
      tryMatch();
    }
    return !cache.isEmpty();
  }

  private void tryMatch() throws PhysicalException {
    Row row;
    if (streamA.hasNext()) {
      row = streamA.next();
    } else if (streamB.hasNext()) {
      row = streamB.next();
    } else {
      throw new IllegalStateException("row stream doesn't have more data!");
    }

    Row targetRow;
    if (hasKey) {
      targetRow = new Row(header, row.getKey(), row.getValues());
    } else {
      targetRow = new Row(header, row.getValues());
    }
    cache.addLast(targetRow);
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return cache.pollFirst();
  }
}
