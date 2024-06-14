package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class InFilter implements Filter {

  String path;

  InOp inOp;

  public enum InOp {
    IN_AND,
    IN_OR,
    NOT_IN_AND,
    NOT_IN_OR;

    public boolean isOrOp() {
      return this == IN_OR || this == NOT_IN_OR;
    }

    public boolean isNotOp() {
      return this == NOT_IN_AND || this == NOT_IN_OR;
    }

    public InOp getOppositeOp() {
      if (this == IN_AND) {
        return NOT_IN_AND;
      } else if (this == IN_OR) {
        return NOT_IN_OR;
      } else if (this == NOT_IN_AND) {
        return IN_AND;
      } else {
        return IN_OR;
      }
    }

    public static InOp str2Op(String str) {
      switch (str.toLowerCase()) {
        case "|in":
        case "in":
          return IN_OR;
        case "|not in":
        case "not in":
        case "|!in":
        case "!in":
          return NOT_IN_OR;
        case "&in":
          return IN_AND;
        case "&not in":
        case "&!in":
          return NOT_IN_AND;
        default:
          throw new SQLParserException("Unsupported InOp: " + str);
      }
    }

    @Override
    public String toString() {
      switch (this) {
        case IN_AND:
          return "&in";
        case IN_OR:
          return "in";
        case NOT_IN_AND:
          return "&not in";
        case NOT_IN_OR:
          return "not in";
        default:
          throw new SQLParserException("Unsupported InOp: " + this);
      }
    }
  }

  Set<Value> values = new HashSet<>();

  public InFilter(String path, InOp inOp, Collection<Value> values) {
    this.path = path;
    this.values.addAll(values);
    this.inOp = inOp;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public Set<Value> getValues() {
    return values;
  }

  public InOp getInOp() {
    return inOp;
  }

  public void setInOp(InOp inOp) {
    this.inOp = inOp;
  }

  public void reverseFunc() {
    this.inOp = inOp.getOppositeOp();
  }

  public boolean validate(Value value) {
    return values.contains(value) ^ inOp.isNotOp();
  }

  @Override
  public void accept(FilterVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public FilterType getType() {
    return FilterType.In;
  }

  @Override
  public Filter copy() {
    return new InFilter(path, inOp, new HashSet<>(values));
  }

  @Override
  public String toString() {
    return String.format("%s %s (%s)", path, inOp, values.toString().substring(1, values.toString().length() - 1));
  }
}
