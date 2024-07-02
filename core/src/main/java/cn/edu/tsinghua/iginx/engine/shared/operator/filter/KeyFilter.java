package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import java.util.Objects;

public class KeyFilter implements Filter {

  private final FilterType type = FilterType.Key;

  private final long value;
  private Op op;

  public KeyFilter(Op op, long value) {
    this.op = op;
    this.value = value;
  }

  public void reverseFunc() {
    this.op = Op.getOpposite(op);
  }

  public Op getOp() {
    return op;
  }

  public long getValue() {
    return value;
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public FilterType getType() {
    return type;
  }

  @Override
  public Filter copy() {
    return new KeyFilter(op, value);
  }

  @Override
  public String toString() {
    return "key " + Op.op2Str(op) + " " + value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyFilter that = (KeyFilter) o;
    return value == that.value && type == that.type && op == that.op;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value, op);
  }
}
