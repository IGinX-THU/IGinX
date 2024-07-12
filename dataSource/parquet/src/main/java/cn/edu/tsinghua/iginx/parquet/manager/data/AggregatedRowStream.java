package cn.edu.tsinghua.iginx.parquet.manager.data;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AggregatedRowStream implements RowStream {

  private final Row row;
  private boolean hasNext = true;

  public AggregatedRowStream(Map<String, ?> values, String functionName) {
    List<Field> fieldList = new ArrayList<>(values.size());
    List<Object> valuesList = new ArrayList<>(values.size());
    for (Map.Entry<String, ?> entry : values.entrySet()) {
      Object value = entry.getValue();
      valuesList.add(value);

      Map.Entry<String, Map<String, String>> pathWithTags =
          DataViewWrapper.parseFieldName(entry.getKey());
      String path = pathWithTags.getKey();
      Map<String, String> tags = pathWithTags.getValue();
      String pathWithFunctionName = functionName + "(" + path + ")";

      DataType dataType = typeFromValue(value);
      Field field = new Field(pathWithFunctionName, dataType, tags);
      fieldList.add(field);
    }
    row = new Row(new Header(fieldList), valuesList.toArray());
  }

  private static DataType typeFromValue(Object value) {
    if (value instanceof Long) {
      return DataType.LONG;
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + value.getClass().getName());
    }
  }

  @Override
  public Header getHeader() throws PhysicalException {
    return row.getHeader();
  }

  @Override
  public void close() throws PhysicalException {}

  @Override
  public boolean hasNext() throws PhysicalException {
    return hasNext;
  }

  @Override
  public Row next() throws PhysicalException {
    hasNext = false;
    return row;
  }
}
