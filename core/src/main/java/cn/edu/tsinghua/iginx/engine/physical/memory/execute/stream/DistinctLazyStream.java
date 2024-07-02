package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import static cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils.isEqualRow;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class DistinctLazyStream extends UnaryLazyStream {

  private final Deque<Row> cache;

  private final HashMap<Integer, List<Row>> rowsHashMap;
  private final List<Row> nullValueRows;

  public DistinctLazyStream(RowStream stream) {
    super(stream);
    this.cache = new LinkedList<>();
    this.rowsHashMap = new HashMap<>();
    this.nullValueRows = new ArrayList<>();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    while (cache.isEmpty() && (stream.hasNext())) {
      update();
    }
    return !cache.isEmpty();
  }

  public void update() throws PhysicalException {
    Row row;
    if (stream.hasNext()) {
      row = stream.next();
    } else {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Value value = row.getAsValue(row.getField(0).getName());

    if (value.isNull()) {
      for (Row nullValueRow : nullValueRows) {
        if (isEqualRow(row, nullValueRow, false)) {
          return;
        }
      }
      nullValueRows.add(row);
      cache.addLast(row);
    } else {
      int hash;
      if (value.getDataType() == DataType.BINARY) {
        hash = Arrays.hashCode(value.getBinaryV());
      } else {
        hash = value.getValue().hashCode();
      }
      if (rowsHashMap.containsKey(hash)) {
        List<Row> rowsExist = rowsHashMap.get(hash);
        for (Row rowExist : rowsExist) {
          if (isEqualRow(row, rowExist, false)) {
            return;
          }
        }
        rowsExist.add(row);
      } else {
        rowsHashMap.put(hash, new ArrayList<>(Collections.singletonList(row)));
      }
      cache.addLast(row);
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
