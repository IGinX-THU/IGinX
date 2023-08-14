package cn.edu.tsinghua.iginx.redis.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.redis.tools.DataTransformer;
import cn.edu.tsinghua.iginx.redis.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class RedisQueryRowStream implements RowStream {

  private List<Column> columns;

  private List<Long> times;

  private final Header header;

  private final Filter filter;

  private Row nextRow = null;

  private int cur = 0;

  public RedisQueryRowStream(List<Column> columns, Filter filter) {
    this.columns = columns;

    Set<Long> timeSet = new TreeSet<>();
    List<Field> fields = new ArrayList<>();
    for (Column column : columns) {
      Pair<String, Map<String, String>> pair = TagKVUtils.splitFullName(column.getPathName());
      fields.add(new Field(pair.getK(), column.getType(), pair.getV()));
      timeSet.addAll(column.getData().keySet());
    }
    this.times = new ArrayList<>(timeSet);
    this.header = new Header(Field.KEY, fields);
    this.filter = filter;
  }

  public RedisQueryRowStream(List<Column> columns) {
    this(columns, null);
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
    if (nextRow == null && cur < times.size()) {
      nextRow = calculateNext();
    }
    return nextRow != null;
  }

  @Override
  public Row next() throws PhysicalException {
    if (!hasNext()) {
      throw new PhysicalException("no more data");
    }

    Row currRow = nextRow;
    nextRow = calculateNext();
    return currRow;
  }

  private Row calculateNext() throws PhysicalException {
    while (cur < times.size()) {
      long timestamp = times.get(cur);
      cur++;

      Object[] values = new Object[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        String strVal = columns.get(i).getData().get(timestamp);
        DataType type = columns.get(i).getType();
        values[i] = DataTransformer.strValueToDeterminedType(strVal, type);
      }

      Row row = new Row(header, timestamp, values);
      if (filter == null || FilterUtils.validate(filter, row)) return row;
    }

    return null;
  }
}
