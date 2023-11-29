package cn.edu.tsinghua.iginx.parquet.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.Column;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.util.*;

public class ScannerRowStream implements RowStream {

  private final Map<String, Integer> indexes;

  private final Header header;

  private final Scanner<Long, Scanner<String, Object>> scanner;

  private Row nextRow;

  public ScannerRowStream(
      Map<String, Column> projected, Scanner<Long, Scanner<String, Object>> scanner) {
    this.indexes = new HashMap<>();
    List<Field> fieldList = new ArrayList<>(projected.size());
    for (Map.Entry<String, Column> entry : projected.entrySet()) {
      indexes.put(entry.getKey(), indexes.size());
      Column column = entry.getValue();
      fieldList.add(new Field(entry.getKey(), column.getDataType(), column.getTags()));
    }
    this.header = new Header(Field.KEY, fieldList);
    this.scanner = scanner;
    this.nextRow = null;
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    this.scanner.close();
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    if (nextRow == null) {
      if (scanner.iterate()) {
        long key = scanner.key();
        Object[] values = new Object[indexes.size()];
        while (scanner.value().iterate()) {
          Integer index = indexes.get(scanner.value().key());
          assert index != null;
          values[index] = scanner.value().value();
        }
        nextRow = new Row(header, key, values);
      }
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("No more rows");
    }
    Row row = nextRow;
    nextRow = null;
    return row;
  }
}
