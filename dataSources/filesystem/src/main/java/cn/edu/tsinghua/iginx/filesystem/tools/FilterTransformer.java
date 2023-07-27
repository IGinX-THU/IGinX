package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.filesystem.thrift.FSFilter;
import cn.edu.tsinghua.iginx.filesystem.thrift.Op;
import cn.edu.tsinghua.iginx.filesystem.thrift.Value;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import com.alibaba.fastjson2.JSONB;
import com.alibaba.fastjson2.JSONReader;
import com.alibaba.fastjson2.JSONWriter;
import com.google.common.collect.BiMap;
import java.util.*;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.*;
import static cn.edu.tsinghua.iginx.filesystem.thrift.FilterType.*;

public class FilterTransformer {
//  private static int index = 0;
//  private static int deep = 0;
//  private static String prefix = "A";

  public static String toString(Filter filter) {
    if (filter == null) {
      return null;
    }
    return new String(JSONB.toBytes(filter, JSONWriter.Feature.WriteClassName));
  }

  public static Filter toFilter(String filter) {
    return JSONB.parseObject(filter.getBytes(), Filter.class);
//    return null;
  }

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
      case Path:
        return toFSFilter((PathFilter) filter);
      default:
        return null;
    }
  }

  private static FSFilter toFSFilter(AndFilter filter) {
    FSFilter fsFilter = new FSFilter(And);
    for (Filter f : filter.getChildren()) {
      fsFilter.addToChildren(toFSFilter(f));
    }
    return fsFilter;
  }

  private static FSFilter toFSFilter(PathFilter filter) {
    FSFilter fsFilter = new FSFilter(Path);
    fsFilter.setPathA(filter.getPathA());
    fsFilter.setPathB(filter.getPathB());
    fsFilter.setOp(toFSOp(filter.getOp()));
    return fsFilter;
  }

  private static FSFilter toFSFilter(OrFilter filter) {
    FSFilter fsFilter = new FSFilter(Or);
    for (Filter f : filter.getChildren()) {
      fsFilter.addToChildren(toFSFilter(f));
    }
    return fsFilter;
  }

  private static FSFilter toFSFilter(NotFilter filter) {
    FSFilter fsFilter = new FSFilter(Not);
    fsFilter.addToChildren(toFSFilter(filter.getChild()));
    return fsFilter;
  }

  private static FSFilter toFSFilter(KeyFilter filter) {
    FSFilter fsFilter = new FSFilter(Key);
    fsFilter.setOp(toFSOp(filter.getOp()));
    fsFilter.setKeyValue(filter.getValue());
    return fsFilter;
  }

  private static FSFilter toFSFilter(ValueFilter filter) {
    FSFilter fsFilter = new FSFilter(Value);
    fsFilter.setValue(toFSValue(filter.getValue()));
    fsFilter.setPath(filter.getPath());
    fsFilter.setOp(toFSOp(filter.getOp()));
    return fsFilter;
  }

  private static FSFilter toFSFilter(BoolFilter filter) {
    FSFilter fsFilter = new FSFilter(Bool);
    fsFilter.setIsTrue(filter.isTrue());
    return fsFilter;
  }

  private static Op toFSOp(cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op op) {
    switch (op) {
      case L:
        return Op.L;
      case LE:
        return Op.LE;
      case LIKE:
        return Op.LIKE;
      case NE:
        return Op.NE;
      case E:
        return Op.E;
      case GE:
        return Op.GE;
      case G:
        return Op.G;
      default:
        return Op.UNKNOW;
    }
  }

  private static cn.edu.tsinghua.iginx.filesystem.thrift.Value toFSValue(cn.edu.tsinghua.iginx.engine.shared.data.Value value) {
    Value fsValue = new Value();
    fsValue.setDataType(value.getDataType());
    switch (value.getDataType()) {
      case FLOAT:
        fsValue.setFloatV(value.getFloatV());
        break;
      case INTEGER:
        fsValue.setIntV(value.getIntV());
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
      case LONG:
        fsValue.setLongV(value.getLongV());
        break;
    }
    return fsValue;
  }








  public static Filter toFilter(FSFilter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return toAndFilter(filter);
      case Or:
        return toOrFilter(filter);
      case Not:
        return toNotFilter( filter);
      case Value:
        return toValueFilter( filter);
      case Key:
        return toKeyFilter(filter);
      case Bool:
        return toBoolFilter( filter);
      case Path:
        return toPathFilter( filter);
      default:
        return null;
    }
  }

  private static Filter toAndFilter(FSFilter andFilter) {
    List<Filter> filters = new ArrayList<>();
    for (FSFilter f : andFilter.getChildren()) {
      filters.add(toFilter(f));
    }
    return new AndFilter(filters);
  }

  private static Filter toPathFilter(FSFilter filter) {
    return new PathFilter(filter.getPathA(),toOp(filter.getOp()),filter.getPathB());
  }

  private static Filter toOrFilter(FSFilter filter) {
    List<Filter> filters = new ArrayList<>();
    for (FSFilter f : filter.getChildren()) {
      filters.add(toFilter(f));
    }
    return new OrFilter(filters);
  }

  private static Filter toNotFilter(FSFilter filter) {
    return new NotFilter(toFilter(filter.getChildren().get(0)));
  }

  private static Filter toKeyFilter(FSFilter filter) {
    return new KeyFilter(toOp(filter.getOp()),filter.getKeyValue());
  }

  private static Filter toValueFilter(FSFilter filter) {
    return new ValueFilter(filter.getPath(),toOp(filter.getOp()),toValue(filter.getValue()));
  }

  private static Filter toBoolFilter(FSFilter filter) {
    return new BoolFilter(filter.isIsTrue());
  }

  private static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op toOp(Op op) {
    switch (op) {
      case L:
        return L;
      case LE:
        return LE;
      case LIKE:
        return LIKE;
      case NE:
        return NE;
      case E:
        return E;
      case GE:
        return GE;
      case G:
        return G;
      default:
        return null;
    }
  }

  private static cn.edu.tsinghua.iginx.engine.shared.data.Value toValue(Value value) {
    cn.edu.tsinghua.iginx.engine.shared.data.Value myValue = null;
    switch (value.getDataType()) {
      case FLOAT:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.getFloatV());
        break;
      case INTEGER:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.getIntV());
        break;
      case BINARY:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.getBinaryV());
        break;
      case BOOLEAN:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.isBoolV());
        break;
      case DOUBLE:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.getDoubleV());
        break;
      case LONG:
        myValue = new cn.edu.tsinghua.iginx.engine.shared.data.Value(value.getLongV());
        break;
    }
    return myValue;
  }


  // 用于表达式求值
//  public static String toString(Filter filter, BiMap<String, String> vals) {
//    if (filter == null) {
//      return "";
//    }
//    deep++;
//    switch (filter.getType()) {
//      case And:
//        return toAndString((AndFilter) filter, vals);
//      case Or:
//        return toOrString((OrFilter) filter, vals);
//      case Not:
//        return toNotString((NotFilter) filter, vals);
//      case Value:
//        return toValueString((ValueFilter) filter, vals);
//      case Key:
//        return toKeyString((KeyFilter) filter, vals);
//      default:
//        return "";
//    }
//  }
//
//  private static String toAndString(AndFilter filter, BiMap<String, String> vals) {
//    String res = "(";
//    for (Filter f : filter.getChildren()) {
//      res += toString(f, vals);
//      res += "&";
//    }
//    if (res.length() != 1) res = res.substring(0, res.length() - 1);
//    res += ")";
//    refreshIndex();
//    return res;
//  }
//
//  private static String toNotString(NotFilter filter, BiMap<String, String> vals) {
//    refreshIndex();
//    return "!" + toString(filter.getChild(), vals);
//  }
//
//  private static String toKeyString(KeyFilter filter, BiMap<String, String> vals) {
//    String val = "key" + " " + op2Str(filter.getOp())+ " " + filter.getValue();
//    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
//    refreshIndex();
//    return vals.inverse().get(val);
//  }
//
//  private static String toValueString(ValueFilter filter, BiMap<String, String> vals) {
//    String val;
//    if (filter.getOp().equals(LIKE)) {
//      val = filter.getPath() + " like " + filter.getValue().getBinaryVAsString();
//    } else {
//      val = filter.getPath() + " " + op2Str(filter.getOp()) + " " + filter.getValue();
//    }
//    if (!vals.containsValue(val)) vals.put(prefix + (index++), val);
//    refreshIndex();
//    return vals.inverse().get(val);
//  }
//
//  private static String toOrString(OrFilter filter, BiMap<String, String> vals) {
//    String res = "(";
//    for (Filter f : filter.getChildren()) {
//      res += toString(f, vals);
//      res += "|";
//    }
//    if (res.length() != 1) res = res.substring(0, res.length() - 1);
//    res += ")";
//    refreshIndex();
//    return res;
//  }
//
//  private static final void refreshIndex() {
//    deep--;
//    if (deep == 0) {
//      index = 0;
//    }
//  }
}
