package cn.edu.tsinghua.iginx.parquet.util.exception;

public class TypeConflictedException extends SchemaException {
  private final String field;
  private final String type;
  private final String oldType;

  public TypeConflictedException(String field, String type, String oldType) {
    super(String.format("can't insert %s value into %s column at %s", type, oldType, field));
    this.field = field;
    this.type = type;
    this.oldType = oldType;
  }

  public String getField() {
    return field;
  }

  public String getType() {
    return type;
  }

  public String getOldType() {
    return oldType;
  }
}
