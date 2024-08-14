package cn.edu.tsinghua.iginx.filestore.test;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RowsBuilder {
  private final List<String> fields;

  public RowsBuilder(List<String> header) {
    this.fields = Objects.requireNonNull(header);
  }

  public RowsBuilder(String... header) {
    this(Arrays.asList(header));
  }

  private final List<Row> rows = new ArrayList<>();

  public List<Row> build() {
    return rows;
  }

  private Header header = null;

  private void initHeader(Object... values) {
    if (header != null) {
      return;
    }
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      fields.add(new Field(this.fields.get(i), parseType(values[i])));
    }
    header = new Header(Field.KEY, fields);
  }

  private static DataType parseType(Object object) {
    if (object instanceof Integer) {
      return DataType.INTEGER;
    } else if (object instanceof Long) {
      return DataType.LONG;
    } else if (object instanceof Float) {
      return DataType.FLOAT;
    } else if (object instanceof Double) {
      return DataType.DOUBLE;
    } else if (object instanceof String) {
      return DataType.BINARY;
    } else {
      throw new IllegalArgumentException("Unsupported type: " + object.getClass());
    }
  }

  public RowsBuilder add(long key, Object... values) {
    initHeader(values);
    Row row = new Row(header, key, values);
    rows.add(row);
    return this;
  }
}
