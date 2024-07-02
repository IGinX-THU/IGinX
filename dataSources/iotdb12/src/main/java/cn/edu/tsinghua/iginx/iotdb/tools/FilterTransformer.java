package cn.edu.tsinghua.iginx.iotdb.tools;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.isLikeOp;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.thrift.DataType;

public class FilterTransformer {

  public static String toString(Filter filter) {
    if (filter == null) {
      return "";
    }
    switch (filter.getType()) {
      case And:
        return toString((AndFilter) filter);
      case Or:
        return toString((OrFilter) filter);
      case Not:
        return toString((NotFilter) filter);
      case Value:
        return toString((ValueFilter) filter);
      case Key:
        return toString((KeyFilter) filter);
      default:
        return "";
    }
  }

  private static String toString(AndFilter filter) {
    StringBuilder sb = new StringBuilder();
    for (Filter child : filter.getChildren()) {
      String filterStr = toString(child);
      if (!filterStr.isEmpty()) {
        sb.append(toString(child)).append(" and ");
      }
    }

    if (sb.length() == 0) {
      return "";
    }

    return "(" + sb.substring(0, sb.length() - 4) + ")";
  }

  private static String toString(NotFilter filter) {
    String childStr = toString(filter.getChild());

    if (childStr.isEmpty()) {
      return "";
    }

    return "not " + childStr;
  }

  private static String toString(KeyFilter filter) {
    return "time " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + filter.getValue();
  }

  private static String toString(ValueFilter filter) {
    String value =
        filter.getValue().getDataType() == DataType.BINARY
            ? "'" + filter.getValue().getBinaryVAsString() + "'"
            : filter.getValue().getValue().toString();

    if (isLikeOp(filter.getOp())) {
      if (!value.endsWith("$'")) {
        value = value.substring(0, value.length() - 1) + "$'";
      }
      return filter.getPath() + " regexp " + value;
    }

    return filter.getPath() + " " + Op.op2StrWithoutAndOr(filter.getOp()) + " " + value;
  }

  private static String toString(OrFilter filter) {
    StringBuilder sb = new StringBuilder();
    for (Filter child : filter.getChildren()) {
      String filterStr = toString(child);
      if (!filterStr.isEmpty()) {
        sb.append(toString(child)).append(" or ");
      }
    }

    if (sb.length() == 0) {
      return "";
    }

    return "(" + sb.substring(0, sb.length() - 4) + ")";
  }
}
