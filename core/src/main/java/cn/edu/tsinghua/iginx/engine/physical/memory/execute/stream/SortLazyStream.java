package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import java.util.ArrayList;
import java.util.List;

public class SortLazyStream extends UnaryLazyStream {

  private final Sort sort;

  private final boolean asc;

  private final List<Row> rows;

  private boolean hasSorted = false;

  private int cur = 0;

  public SortLazyStream(Sort sort, RowStream stream) {
    super(stream);
    this.sort = sort;
    this.asc = sort.getSortType() == Sort.SortType.ASC;
    this.rows = new ArrayList<>();
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return stream.getHeader();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (!hasSorted) {
      while (stream.hasNext()) {
        rows.add(stream.next());
      }
      RowUtils.sortRows(rows, asc, sort.getSortByCols());
      hasSorted = true;
    }
    return cur < rows.size();
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new IllegalStateException("row stream doesn't have more data!");
    }
    return rows.get(cur++);
  }
}
