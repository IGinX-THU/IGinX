package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class AddSchemaPrefix extends AbstractUnaryOperator {

  private final String schemaPrefix; // 可以为 null

  public AddSchemaPrefix(Source source, String schemaPrefix) {
    super(OperatorType.AddSchemaPrefix, source);
    this.schemaPrefix = schemaPrefix;
  }

  @Override
  public Operator copy() {
    return new AddSchemaPrefix(getSource().copy(), schemaPrefix);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new AddSchemaPrefix(source, schemaPrefix);
  }

  @Override
  public String getInfo() {
    return "SchemaPrefix: " + schemaPrefix;
  }

  public String getSchemaPrefix() {
    return schemaPrefix;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    AddSchemaPrefix that = (AddSchemaPrefix) object;
    return schemaPrefix.equals(that.schemaPrefix);
  }
}
