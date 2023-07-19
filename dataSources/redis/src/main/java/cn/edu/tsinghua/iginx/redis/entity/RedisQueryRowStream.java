package cn.edu.tsinghua.iginx.redis.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.redis.tools.DataTransformer;
import cn.edu.tsinghua.iginx.redis.tools.TagKVUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class RedisQueryRowStream implements RowStream {

  private List<Column> columns;

  private List<Long> times;

  private final Header header;

  private int cur = 0;

  public RedisQueryRowStream(List<Column> columns) {
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

    long timestamp = times.get(cur);
    cur++;

    Object[] values = new Object[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      String strVal = columns.get(i).getData().get(timestamp);
      DataType type = columns.get(i).getType();
      values[i] = DataTransformer.strValueToDeterminedType(strVal, type);
    }
    return new Row(header, timestamp, values);
  }
}
