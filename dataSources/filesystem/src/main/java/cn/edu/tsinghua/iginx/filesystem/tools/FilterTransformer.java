package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.FSFilter;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import com.google.common.collect.BiMap;
import java.util.*;
import java.util.stream.Collectors;

public class FilterTransformer {
  private static int index = 0;
  private static int deep = 0;
  private static String prefix = "A";

  public static FSFilter toFSFilter(Filter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return toFSFilter((AndFilter) filter);
      case Or:
        return toFSFilter((OrFilter) filter);
      case Not:
        return toFSFilter((NotFilter) filter);
      case Value:
        return toFSFilter((ValueFilter) filter);
      case Key:
        return toFSFilter((KeyFilter) filter);
      case Bool:
        return toFSFilter((BoolFilter) filter);
      default:
        return null;
    }
  }

  private static FSFilter toFSFilter(AndFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.And);
    for (Filter f : filter.getChildren()){
      fsFilter.addToChildren(toFSFilter(f));
    }
    return fsFilter;
  }

  private static FSFilter toFSFilter(OrFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Or);
    for (Filter f : filter.getChildren()){
      fsFilter.addToChildren(toFSFilter(f));
    }
    return fsFilter;
  }

  private static FSFilter toFSFilter(NotFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Not);
    fsFilter.addToChildren(toFSFilter(filter.getChild()));
    return fsFilter;
  }

  private static FSFilter toFSFilter(KeyFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Key);
    fsFilter.setOp(toFSOp(filter.getOp()));
    fsFilter.setKeyValue(filter.getValue());
    return fsFilter;
  }

  private static FSFilter toFSFilter(ValueFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Value);
    fsFilter.setValue(toFSValue(filter.getValue()));
    fsFilter.setPath(filter.getPath());
    fsFilter.setOp(toFSOp(filter.getOp()));
    return fsFilter;
  }

  private static FSFilter toFSFilter(BoolFilter filter) {
    FSFilter fsFilter = new FSFilter(cn.edu.tsinghua.iginx.common.thrift.FilterType.Bool);
    fsFilter.setIsTrue(filter.isTrue());
    return fsFilter;
  }

  private static cn.edu.tsinghua.iginx.common.thrift.Op toFSOp(Op op) {
    switch (op){
      case L:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.L;
      case LE:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.L;
      case LIKE:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.LIKE;
      case NE:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.NE;
      case GE:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.GE;
      case G:
        return  cn.edu.tsinghua.iginx.common.thrift.Op.G;
      default:
        return cn.edu.tsinghua.iginx.common.thrift.Op.UNKNOW;
    }
  }

  private static cn.edu.tsinghua.iginx.common.thrift.Value toFSValue(Value value) {
    cn.edu.tsinghua.iginx.common.thrift.Value fsValue = new cn.edu.tsinghua.iginx.common.thrift.Value();
    fsValue.setDataType(value.getDataType());
    switch (value.getDataType()){
      case FLOAT:
        fsValue.setFloatV(value.getFloatV());
        break;
      case INTEGER:
        fsValue.setLongV(value.getLongV());
        break;
      case BINARY:
        fsValue.setBinaryV(value.getBinaryV());
        break;
      case BOOLEAN:
        fsValue.setBoolV(value.getBoolV());
        break;
      case DOUBLE:
        fsValue.setDoubleV(value.getDoubleV());
        break;
    }
    return fsValue;
  }


  // 用于表达式求值
  public static String toString(FSFilter filter, BiMap<String, String> vals) {
    if (filter == null) {
      return "";
    }
    deep++;
    switch (filter.getType()) {
      case And:
        return toAndString(filter, vals);
      case Or:
        return toOrString(filter, vals);
      case Not:
        return toNotString(filter, vals);
      case Value:
        return toValueString(filter, vals);
      case Key:
        return toKeyString(filter, vals);
      default:
        return "";
    }
  }

  private static String toAndString(FSFilter filter, BiMap<String, String> vals) {
    String res = "(";
    for (FSFilter f : filter.getChildren()) {
      res += toString(f, vals);
      res += "&";
    }
    if (res.length() != 1) res = res.substring(0, res.length() - 1);
    res += ")";
    refreshIndex();
    return res;
  }

  private static String toNotString(FSFilter filter, BiMap<String, String> vals) {
    refreshIndex();
    return "!" + toString(filter.getChildren().get(0), vals);
  }

  private static String toKeyString(FSFilter filter, BiMap<String, String> vals) {
    String val = "key" + " " + fsOp2Str(filter.getOp()) + " " + filter.getKeyValue();
    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
    refreshIndex();
    return vals.inverse().get(val);
  }

  private static String toValueString(FSFilter filter, BiMap<String, String> vals) {
    String val;
    if (filter.getOp().equals(cn.edu.tsinghua.iginx.common.thrift.Op.LIKE)) {
      val = filter.getPath() + " like " + filter.getValue().binaryV.toString();
    } else {
      val =
          filter.getPath()
              + " "
              + fsOp2Str(filter.getOp())
              + " "
              + filter.getValue();
    }
    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
    refreshIndex();
    return vals.inverse().get(val);
  }

  private static String toOrString(FSFilter filter, BiMap<String, String> vals) {
    String res = "(";
    for (FSFilter f : filter.getChildren()) {
      res += toString(f, vals);
      res += "|";
    }
    if (res.length() != 1) res = res.substring(0, res.length() - 1);
    res += ")";
    refreshIndex();
    return res;
  }

  private static String fsOp2Str(cn.edu.tsinghua.iginx.common.thrift.Op op) {
    switch (op) {
      case GE:
        return ">=";
      case G:
        return ">";
      case LE:
        return "<=";
      case L:
        return "<";
      case E:
        return "==";
      case NE:
        return "!=";
      case LIKE:
        return "like";
      default:
        return "";
    }
  }

  private static final void refreshIndex() {
    deep--;
    if (deep == 0) {
      index = 0;
    }
  }
}