package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Limit extends AbstractUnaryOperator {

  private final int limit;

  private final int offset;

  public Limit(Source source, int limit, int offset) {
    super(OperatorType.Limit, source);
    if (limit < 0 || offset < 0) {
      throw new IllegalArgumentException("limit and offset shouldn't less than zero");
    }
    this.limit = limit;
    this.offset = offset;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public Operator copy() {
    return new Limit(getSource().copy(), limit, offset);
  }

  @Override
  public UnaryOperator copyWithSource(Source source) {
    return new Limit(source, limit, offset);
  }

  @Override
  public String getInfo() {
    return "Limit: " + limit + ", Offset: " + offset;
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Limit limit1 = (Limit) object;
    return limit == limit1.limit && offset == limit1.offset;
  }
}
