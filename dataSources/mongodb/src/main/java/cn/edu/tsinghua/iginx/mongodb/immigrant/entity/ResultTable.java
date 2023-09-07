package cn.edu.tsinghua.iginx.mongodb.immigrant.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.*;

public class ResultTable implements RowStream {

  private final Header header;

  private final Iterator<Map.Entry<Long, Object[]>> rowItr;

  public ResultTable(Header header, Iterator<Map.Entry<Long, Object[]>> rowItr) {
    this.header = header;
    this.rowItr = rowItr;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return this.header;
  }

  @Override
  public void close() throws PhysicalException {
    while (this.rowItr.hasNext()) {
      this.rowItr.next();
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return this.rowItr.hasNext();
  }

  @Override
  public Row next() throws PhysicalException {
    Map.Entry<Long, Object[]> rowData = this.rowItr.next();
    return new Row(this.header, rowData.getKey(), rowData.getValue());
  }

  public static ColumnsBuilder columnsBuilder(int columnNum) {
    return new ColumnsBuilder(columnNum);
  }

  public static class ColumnsBuilder {

    private final int columnNum;

    private List<Field> fieldList;

    private SortedMap<Long, Object[]> rows;

    private ColumnsBuilder(int columnNum) {
      this.columnNum = columnNum;
      this.fieldList = new ArrayList<>(columnNum);
      this.rows = new TreeMap<>();
    }

    public void add(Field field) {
      this.fieldList.add(field);
    }

    public void put(Long key, Object value) {
      int idx = fieldList.size() - 1;
      Object[] row = rows.computeIfAbsent(key, k -> new Object[this.columnNum]);
      row[idx] = value;
    }

    public ResultTable build() {
      Header header = new Header(Field.KEY, new ArrayList<>(this.fieldList));
      return new ResultTable(header, rows.entrySet().iterator());
    }
  }
}
