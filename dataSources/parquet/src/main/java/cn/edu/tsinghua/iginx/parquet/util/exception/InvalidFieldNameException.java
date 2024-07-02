package cn.edu.tsinghua.iginx.parquet.util.exception;

public class InvalidFieldNameException extends SchemaException {

  private final String fieldName;

  private final String reason;

  public InvalidFieldNameException(String fieldName, String reason) {
    super(String.format("invalid field name %s, because: ", fieldName, reason));
    this.fieldName = fieldName;
    this.reason = reason;
  }

  public String getFieldName() {
    return fieldName;
  }

  public String getReason() {
    return reason;
  }
}
