package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.FSFilter;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import com.google.common.collect.BiMap;
import java.util.*;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.LIKE;
import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.op2Str;

public class FilterTransformer {
  private static int index = 0;
  private static int deep = 0;
  private static String prefix = "A";

  public static String toString(Filter filter) {
    if (filter == null) {
      return null;
    }
    return new String(JsonUtils.toJsonWithClassName(filter));
  }

  public static Filter toFilter(String filter) {
    return JsonUtils.fromJson(filter.getBytes(), Filter.class);
  }

//  public static FSFilter toFSFilter(Filter filter) {
//    if (filter == null) {
//      return null;
//    }
//    switch (filter.getType()) {
//      case And:
//        return toFSFilter((AndFilter) filter);
//      case Or:
//        return toFSFilter((OrFilter) filter);
//      case Not:
//        return toFSFilter((NotFilter) filter);
//      case Value:
//        return toFSFilter((ValueFilter) filter);
//      case Key:
//        return toFSFilter((KeyFilter) filter);
//      case Bool:
//        return toFSFilter((BoolFilter) filter);
//      default:
//        return null;
//    }
//  }
//
//  private static FSFilter toFSFilter(AndFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.And);
//    for (Filter f : filter.getChildren()) {
//      fsFilter.addToChildren(toFSFilter(f));
//    }
//    return fsFilter;
//  }
//
//  private static FSFilter toFSFilter(OrFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Or);
//    for (Filter f : filter.getChildren()) {
//      fsFilter.addToChildren(toFSFilter(f));
//    }
//    return fsFilter;
//  }
//
//  private static FSFilter toFSFilter(NotFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Not);
//    fsFilter.addToChildren(toFSFilter(filter.getChild()));
//    return fsFilter;
//  }
//
//  private static FSFilter toFSFilter(KeyFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Key);
//    fsFilter.setOp(toFSOp(filter.getOp()));
//    fsFilter.setKeyValue(filter.getValue());
//    return fsFilter;
//  }
//
//  private static FSFilter toFSFilter(ValueFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Value);
//    fsFilter.setValue(toFSValue(filter.getValue()));
//    fsFilter.setPath(filter.getPath());
//    fsFilter.setOp(toFSOp(filter.getOp()));
//    return fsFilter;
//  }
//
//  private static FSFilter toFSFilter(BoolFilter filter) {
//    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Bool);
//    fsFilter.setIsTrue(filter.isTrue());
//    return fsFilter;
//  }
//
//  private static cn.edu.tsinghua.iginx.common.thrift.Op toFSOp(Op op) {
//    switch (op) {
//      case L:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.L;
//      case LE:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.L;
//      case LIKE:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.LIKE;
//      case NE:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.NE;
//      case GE:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.GE;
//      case G:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.G;
//      default:
//        return cn.edu.tsinghua.iginx.common.thrift.Op.UNKNOW;
//    }
//  }
//
//  private static cn.edu.tsinghua.iginx.common.thrift.Value toFSValue(Value value) {
//    cn.edu.tsinghua.iginx.common.thrift.Value fsValue =
//        new cn.edu.tsinghua.iginx.common.thrift.Value();
//    fsValue.setDataType(value.getDataType());
//    switch (value.getDataType()) {
//      case FLOAT:
//        fsValue.setFloatV(value.getFloatV());
//        break;
//      case INTEGER:
//        fsValue.setLongV(value.getLongV());
//        break;
//      case BINARY:
//        fsValue.setBinaryV(value.getBinaryV());
//        break;
//      case BOOLEAN:
//        fsValue.setBoolV(value.getBoolV());
//        break;
//      case DOUBLE:
//        fsValue.setDoubleV(value.getDoubleV());
//        break;
//    }
//    return fsValue;
//  }

  // 用于表达式求值
  public static String toString(Filter filter, BiMap<String, String> vals) {
    if (filter == null) {
      return "";
    }
    deep++;
    switch (filter.getType()) {
      case And:
        return toAndString((AndFilter) filter, vals);
      case Or:
        return toOrString((OrFilter) filter, vals);
      case Not:
        return toNotString((NotFilter) filter, vals);
      case Value:
        return toValueString((ValueFilter) filter, vals);
      case Key:
        return toKeyString((KeyFilter) filter, vals);
      default:
        return "";
    }
  }

  private static String toAndString(AndFilter filter, BiMap<String, String> vals) {
    String res = "(";
    for (Filter f : filter.getChildren()) {
      res += toString(f, vals);
      res += "&";
    }
    if (res.length() != 1) res = res.substring(0, res.length() - 1);
    res += ")";
    refreshIndex();
    return res;
  }

  private static String toNotString(NotFilter filter, BiMap<String, String> vals) {
    refreshIndex();
    return "!" + toString(filter.getChild(), vals);
  }

  private static String toKeyString(KeyFilter filter, BiMap<String, String> vals) {
    String val = "key" + " " + op2Str(filter.getOp())+ " " + filter.getValue();
    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
    refreshIndex();
    return vals.inverse().get(val);
  }

  private static String toValueString(ValueFilter filter, BiMap<String, String> vals) {
    String val;
    if (filter.getOp().equals(LIKE)) {
      val = filter.getPath() + " like " + filter.getValue().getBinaryVAsString();
    } else {
      val = filter.getPath() + " " + op2Str(filter.getOp()) + " " + filter.getValue();
    }
    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
    refreshIndex();
    return vals.inverse().get(val);
  }

  private static String toOrString(OrFilter filter, BiMap<String, String> vals) {
    String res = "(";
    for (Filter f : filter.getChildren()) {
      res += toString(f, vals);
      res += "|";
    }
    if (res.length() != 1) res = res.substring(0, res.length() - 1);
    res += ")";
    refreshIndex();
    return res;
  }

  private static final void refreshIndex() {
    deep--;
    if (deep == 0) {
      index = 0;
    }
  }
}
