package cn.edu.tsinghua.iginx.parquet.manager.dummy;

import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.Objects;
import java.util.StringJoiner;

@Deprecated
public class Field {
  private final String name;

  private final DataType type;

  public Field(String name, DataType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public DataType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Field field = (Field) o;
    return Objects.equals(name, field.name) && type == field.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Field.class.getSimpleName() + "[", "]")
        .add("name='" + name + "'")
        .add("type=" + type)
        .toString();
  }
}
