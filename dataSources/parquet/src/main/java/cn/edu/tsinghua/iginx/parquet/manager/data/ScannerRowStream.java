package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.parquet.db.util.iterator.Scanner;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ScannerRowStream implements RowStream {

  private final Map<String, Integer> indexes;

  private final Header header;

  private final Scanner<Long, Scanner<String, Object>> scanner;

  private Row nextRow;

  public ScannerRowStream(
      Map<String, DataType> projected, Scanner<Long, Scanner<String, Object>> scanner) {
    this.indexes = new HashMap<>();
    List<Field> fieldList = new ArrayList<>(projected.size());
    for (Map.Entry<String, DataType> entry : projected.entrySet()) {
      indexes.put(entry.getKey(), indexes.size());
      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      fieldList.add(new Field(pathWithTags.getKey(), entry.getValue(), pathWithTags.getValue()));
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
    try {
      scanner.close();
    } catch (Exception e) {
      throw new RowFetchException(e);
    }
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
