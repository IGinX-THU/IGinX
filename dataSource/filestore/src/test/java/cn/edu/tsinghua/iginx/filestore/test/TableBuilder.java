package cn.edu.tsinghua.iginx.filestore.test;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TableBuilder {

  private final Field keyField;
  private final String prefix;
  private List<String> names = null;
  private Header header = null;
  private final List<Row> rows = new ArrayList<>();

  public TableBuilder(boolean hasKey, String prefix) {
    if (hasKey) {
      keyField = Field.KEY;
    } else {
      keyField = null;
    }
    this.prefix = prefix;
  }

  public TableBuilder names(String... names) {
    this.names = Arrays.asList(names);
    this.names.replaceAll(
        name -> {
          if (prefix != null) {
            return prefix + "." + name;
          }
          return name;
        });
    return this;
  }

  public TableBuilder types(DataType... types) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < names.size(); i++) {
      fields.add(new Field(names.get(i), types[i]));
    }
    this.header = new Header(keyField, fields);
    return this;
  }

  public TableBuilder key(long key, Object... values) {
    Row row = new Row(Objects.requireNonNull(header), key, values);
    rows.add(row);
    return this;
  }

  public TableBuilder row(Object... values) {
    Row row = new Row(Objects.requireNonNull(header), values);
    rows.add(row);
    return this;
  }

  public Table build() {
    return new Table(Objects.requireNonNull(header), rows);
  }
}
