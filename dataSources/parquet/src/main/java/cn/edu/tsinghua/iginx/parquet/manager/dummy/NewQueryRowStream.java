package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.storage.domain.ColumnKey;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Deprecated
public class NewQueryRowStream implements RowStream {

  private List<Column> columns;

  private List<Long> times;

  private final Header header;

  private int cur = 0;

  public NewQueryRowStream(List<Column> columns) {
    this.columns = columns;

    Set<Long> timeSet = new TreeSet<>();
    List<Field> fields = new ArrayList<>();
    for (Column column : columns) {
      ColumnKey key =
          cn.edu.tsinghua.iginx.parquet.manager.utils.TagKVUtils.splitFullName(
              column.getPathName());
      Field field;
      field = new Field(key.getPath(), column.getType(), key.getTags());
      fields.add(field);
      timeSet.addAll(column.getData().keySet());
    }
    this.times = new ArrayList<>(timeSet);
    this.header = new Header(Field.KEY, fields);
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return header;
  }

  @Override
  public void close() throws PhysicalException {
    columns.clear();
    times.clear();
    columns = null;
    times = null;
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    return cur < times.size();
  }

  @Override
  public Row next() throws PhysicalException {
    if (cur >= times.size()) {
      throw new PhysicalException("no more data");
    }

    long time = times.get(cur);
    cur++;

    Object[] values = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      values[i] = columns.get(i).getData().get(time);
    }
    return new Row(header, time, values);
  }
}
