package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReorderLazyStream extends UnaryLazyStream {

  private final Reorder reorder;

  private Header header;

  private Map<Integer, Integer> reorderMap;

  private Row nextRow = null;

  public ReorderLazyStream(Reorder reorder, RowStream stream) {
    super(stream);
    this.reorder = reorder;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    if (this.header == null) {
      Header header = stream.getHeader();
      this.reorderMap = new HashMap<>();
      Header.ReorderedHeaderWrapped res =
          header.reorderedHeaderWrapped(reorder.getPatterns(), reorder.getIsPyUDF());
      this.header = res.getHeader();
      this.reorderMap = res.getReorderMap();
    }
    return this.header;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  private Row calculateNext() throws PhysicalException {
    Header header = getHeader();
    List<Field> targetFields = header.getFields();
    if (stream.hasNext()) {
      Row row = stream.next();
      Object[] values = new Object[targetFields.size()];
      for (int i = 0; i < values.length; i++) {
        values[i] = row.getValue(reorderMap.get(i));
      }
      if (header.hasKey()) {
        return new Row(header, row.getKey(), values);
      } else {
        return new Row(header, values);
      }
    }
    return null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }
}
