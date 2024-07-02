package cn.edu.tsinghua.iginx.parquet.server;

import static cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op.*;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.parquet.thrift.RawFilter;
import cn.edu.tsinghua.iginx.parquet.thrift.RawFilterOp;
import cn.edu.tsinghua.iginx.parquet.thrift.RawFilterType;
import cn.edu.tsinghua.iginx.parquet.thrift.RawValue;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.List;

public class FilterTransformer {

  public static RawFilter toRawFilter(Filter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return toRawFilter((AndFilter) filter);
      case Or:
        return toRawFilter((OrFilter) filter);
      case Not:
        return toRawFilter((NotFilter) filter);
      case Value:
        return toRawFilter((ValueFilter) filter);
      case Key:
        return toRawFilter((KeyFilter) filter);
      case Bool:
        return toRawFilter((BoolFilter) filter);
      case Path:
        return toRawFilter((PathFilter) filter);
      default:
        return null;
    }
  }

  private static RawFilter toRawFilter(AndFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.And);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(toRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter toRawFilter(PathFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Path);
    RawFilter.setPathA(filter.getPathA());
    RawFilter.setPathB(filter.getPathB());
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(OrFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Or);
    for (Filter f : filter.getChildren()) {
      RawFilter.addToChildren(toRawFilter(f));
    }
    return RawFilter;
  }

  private static RawFilter toRawFilter(NotFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Not);
    RawFilter.addToChildren(toRawFilter(filter.getChild()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(KeyFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Key);
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    RawFilter.setKeyValue(filter.getValue());
    return RawFilter;
  }

  private static RawFilter toRawFilter(ValueFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Value);
    RawFilter.setValue(toRawValue(filter.getValue()));
    RawFilter.setPath(filter.getPath());
    RawFilter.setOp(toRawFilterOp(filter.getOp()));
    return RawFilter;
  }

  private static RawFilter toRawFilter(BoolFilter filter) {
    RawFilter RawFilter = new RawFilter(RawFilterType.Bool);
    RawFilter.setIsTrue(filter.isTrue());
    return RawFilter;
  }

  private static RawFilterOp toRawFilterOp(Op op) {
    switch (op) {
      case L:
        return RawFilterOp.L;
      case LE:
        return RawFilterOp.LE;
      case LIKE:
        return RawFilterOp.LIKE;
      case NE:
        return RawFilterOp.NE;
      case E:
        return RawFilterOp.E;
      case GE:
        return RawFilterOp.GE;
      case G:
        return RawFilterOp.G;
      case L_AND:
        return RawFilterOp.L_AND;
      case LE_AND:
        return RawFilterOp.LE_AND;
      case LIKE_AND:
        return RawFilterOp.LIKE_AND;
      case NE_AND:
        return RawFilterOp.NE_AND;
      case E_AND:
        return RawFilterOp.E_AND;
      case GE_AND:
        return RawFilterOp.GE_AND;
      case G_AND:
        return RawFilterOp.G_AND;
      default:
        return RawFilterOp.UNKNOWN;
    }
  }

  private static RawValue toRawValue(cn.edu.tsinghua.iginx.engine.shared.data.Value value) {
    RawValue RawValue = new RawValue();
    RawValue.setDataType(value.getDataType().toString());
    switch (value.getDataType()) {
      case FLOAT:
        RawValue.setFloatV(value.getFloatV());
        break;
      case INTEGER:
        RawValue.setIntV(value.getIntV());
        break;
      case BINARY:
        RawValue.setBinaryV(value.getBinaryV());
        break;
      case BOOLEAN:
        RawValue.setBoolV(value.getBoolV());
        break;
      case DOUBLE:
        RawValue.setDoubleV(value.getDoubleV());
        break;
      case LONG:
        RawValue.setLongV(value.getLongV());
        break;
    }
    return RawValue;
  }

  public static Filter toFilter(RawFilter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case And:
        return toAndFilter(filter);
      case Or:
        return toOrFilter(filter);
      case Not:
        return toNotFilter(filter);
      case Value:
        return toValueFilter(filter);
      case Key:
        return toKeyFilter(filter);
      case Bool:
        return toBoolFilter(filter);
      case Path:
        return toPathFilter(filter);
      default:
        return null;
    }
  }

  private static Filter toAndFilter(RawFilter andFilter) {
    List<Filter> filters = new ArrayList<>();
    for (RawFilter f : andFilter.getChildren()) {
      filters.add(toFilter(f));
    }
    return new AndFilter(filters);
  }

  private static Filter toPathFilter(RawFilter filter) {
    return new PathFilter(filter.getPathA(), toOp(filter.getOp()), filter.getPathB());
  }

  private static Filter toOrFilter(RawFilter filter) {
    List<Filter> filters = new ArrayList<>();
    for (RawFilter f : filter.getChildren()) {
      filters.add(toFilter(f));
    }
    return new OrFilter(filters);
  }

  private static Filter toNotFilter(RawFilter filter) {
    return new NotFilter(toFilter(filter.getChildren().get(0)));
  }

  private static Filter toKeyFilter(RawFilter filter) {
    return new KeyFilter(toOp(filter.getOp()), filter.getKeyValue());
  }

  private static Filter toValueFilter(RawFilter filter) {
    return new ValueFilter(filter.getPath(), toOp(filter.getOp()), toValue(filter.getValue()));
  }

  private static Filter toBoolFilter(RawFilter filter) {
    return new BoolFilter(filter.isIsTrue());
  }

  private static Op toOp(RawFilterOp op) {
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
      case L_AND:
        return L_AND;
      case LE_AND:
        return LE_AND;
      case LIKE_AND:
        return LIKE_AND;
      case NE_AND:
        return NE_AND;
      case E_AND:
        return E_AND;
      case GE_AND:
        return GE_AND;
      case G_AND:
        return G_AND;
      default:
        return null;
    }
  }

  private static Value toValue(RawValue RawValue) {
    Value value = null;
    switch (DataType.valueOf(RawValue.getDataType())) {
      case FLOAT:
        value = new Value(RawValue.getFloatV());
        break;
      case INTEGER:
        value = new Value(RawValue.getIntV());
        break;
      case BINARY:
        value = new Value(RawValue.getBinaryV());
        break;
      case BOOLEAN:
        value = new Value(RawValue.isBoolV());
        break;
      case DOUBLE:
        value = new Value(RawValue.getDoubleV());
        break;
      case LONG:
        value = new Value(RawValue.getLongV());
        break;
    }
    return value;
  }
}
