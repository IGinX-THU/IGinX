package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import static cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils.isNumericType;
import static cn.edu.tsinghua.iginx.sql.SQLConstant.DOT;
import static cn.edu.tsinghua.iginx.thrift.DataType.BOOLEAN;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HeaderUtils {

  public static Header constructNewHead(Header header, String markColumn) {
    List<Field> fields = new ArrayList<>(header.getFields());
    fields.add(new Field(markColumn, BOOLEAN));
    return header.hasKey() ? new Header(Field.KEY, fields) : new Header(fields);
  }

  public static Header constructNewHead(Header headerA, Header headerB, boolean remainKeyA) {
    return constructNewHead(headerA, headerB, remainKeyA, Collections.emptyList());
  }

  public static Header constructNewHead(
      Header headerA, Header headerB, boolean remainKeyA, List<String> extraJoinPaths) {
    List<Field> fields = new ArrayList<>(headerA.getFields());
    headerB
        .getFields()
        .forEach(
            fieldB -> {
              if (!extraJoinPaths.contains(fieldB.getName())) {
                fields.add(fieldB);
              }
            });
    return remainKeyA && headerA.hasKey() ? new Header(Field.KEY, fields) : new Header(fields);
  }

  public static Header constructNewHead(
      Header headerA, Header headerB, String prefixA, String prefixB) {
    List<Field> fields = new ArrayList<>();
    if (headerA.hasKey() && prefixA != null) {
      fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
    }
    fields.addAll(headerA.getFields());
    if (headerB.hasKey() && prefixB != null) {
      fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
    }
    fields.addAll(headerB.getFields());
    return new Header(fields);
  }

  public static Header constructNewHead(
      Header headerA, Header headerB, String prefixA, String prefixB, List<String> extraJoinPaths) {
    List<Field> fields = new ArrayList<>();
    if (headerA.hasKey()) {
      fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
    }
    fields.addAll(headerA.getFields());
    if (headerB.hasKey()) {
      fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
    }
    headerB
        .getFields()
        .forEach(
            field -> {
              if (!extraJoinPaths.contains(field.getName())) {
                fields.add(field);
              }
            });
    return new Header(fields);
  }

  public static Header constructNewHead(
      Header headerA,
      Header headerB,
      String prefixA,
      String prefixB,
      boolean cutRight,
      List<String> joinColumns,
      List<String> extraJoinPaths) {
    List<Field> fields = new ArrayList<>();
    if (headerA.hasKey() && prefixA != null) {
      fields.add(new Field(prefixA + DOT + GlobalConstant.KEY_NAME, DataType.LONG));
    }

    if (cutRight) {
      fields.addAll(headerA.getFields());
    } else {
      List<String> joinPathA = new ArrayList<>();
      joinColumns.forEach(
          joinColumn -> {
            joinPathA.add(prefixA + DOT + joinColumn);
          });
      headerA
          .getFields()
          .forEach(
              field -> {
                if (extraJoinPaths.contains(field.getName())) {
                  return;
                } else if (joinPathA.contains(field.getName())) {
                  return;
                }
                fields.add(field);
              });
    }

    if (headerB.hasKey() && prefixB != null) {
      fields.add(new Field(prefixB + DOT + GlobalConstant.KEY_NAME, DataType.LONG));
    }

    if (cutRight) {
      List<String> joinPathB = new ArrayList<>();
      joinColumns.forEach(
          joinColumn -> {
            joinPathB.add(prefixB + DOT + joinColumn);
          });
      headerB
          .getFields()
          .forEach(
              field -> {
                if (extraJoinPaths.contains(field.getName())) {
                  return;
                } else if (joinPathB.contains(field.getName())) {
                  return;
                }
                fields.add(field);
              });
    } else {
      fields.addAll(headerB.getFields());
    }

    return new Header(fields);
  }

  public static Pair<int[], Header> constructNewHead(
      Header headerA,
      Header headerB,
      String prefixA,
      String prefixB,
      List<String> joinColumns,
      boolean cutRight) {
    List<Field> fieldsA = headerA.getFields();
    List<Field> fieldsB = headerB.getFields();
    int[] indexOfJoinColumnsInTable = new int[joinColumns.size()];

    List<Field> fields = new ArrayList<>();
    if (headerA.hasKey()) {
      fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
    }
    if (cutRight) {
      fields.addAll(fieldsA);
      if (headerB.hasKey()) {
        fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
      }
      int i = 0;
      flag:
      for (Field fieldB : fieldsB) {
        for (String joinColumn : joinColumns) {
          if (Objects.equals(fieldB.getName(), prefixB + '.' + joinColumn)) {
            indexOfJoinColumnsInTable[i++] = headerB.indexOf(fieldB);
            continue flag;
          }
        }
        fields.add(fieldB);
      }
    } else {
      int i = 0;
      flag:
      for (Field fieldA : fieldsA) {
        for (String joinColumn : joinColumns) {
          if (Objects.equals(fieldA.getName(), prefixA + '.' + joinColumn)) {
            indexOfJoinColumnsInTable[i++] = headerA.indexOf(fieldA);
            continue flag;
          }
        }
        fields.add(fieldA);
      }
      if (headerB.hasKey()) {
        fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
      }
      fields.addAll(fieldsB);
    }
    return new Pair<>(indexOfJoinColumnsInTable, new Header(fields));
  }

  public static Pair<String, String> calculateHashJoinPath(
      Header headerA,
      Header headerB,
      String prefixA,
      String prefixB,
      Filter filter,
      List<String> joinColumns,
      List<String> extraJoinPaths)
      throws InvalidOperatorParameterException {
    String joinPathA, joinPathB;
    if (!extraJoinPaths.isEmpty()) {
      joinPathA = extraJoinPaths.get(0);
      joinPathB = extraJoinPaths.get(0);
    } else {
      if (!joinColumns.isEmpty()) {
        joinPathA = prefixA + '.' + joinColumns.get(0);
        joinPathB = prefixB + '.' + joinColumns.get(0);
      } else {
        Pair<String, String> pair = calculateHashJoinPath(filter);
        if (pair == null) {
          throw new InvalidOperatorParameterException(
              "filter: " + filter + " can't be used in hash join.");
        }
        if (headerA.indexOf(pair.k) != -1 && headerB.indexOf(pair.v) != -1) {
          joinPathA = pair.k;
          joinPathB = pair.v;
        } else if (headerA.indexOf(pair.v) != -1 && headerB.indexOf(pair.k) != -1) {
          joinPathA = pair.v;
          joinPathB = pair.k;
        } else {
          throw new InvalidOperatorParameterException(
              "invalid hash join path filter input: " + filter);
        }
      }
    }
    return new Pair<>(joinPathA, joinPathB);
  }

  private static Pair<String, String> calculateHashJoinPath(Filter filter) {
    if (filter == null) {
      return null;
    }
    switch (filter.getType()) {
      case Or:
        OrFilter orFilter = (OrFilter) filter;
        if (orFilter.getChildren().size() == 1) {
          return calculateHashJoinPath(orFilter.getChildren().get(0));
        }
        break;
      case And:
        AndFilter andFilter = (AndFilter) filter;
        for (Filter child : andFilter.getChildren()) {
          Pair<String, String> ret = calculateHashJoinPath(child);
          if (ret != null) {
            return ret;
          }
        }
        break;
      case Not:
        NotFilter notFilter = (NotFilter) filter;
        return calculateHashJoinPath(notFilter.getChild());
      case Path:
        PathFilter pathFilter = (PathFilter) filter;
        if (pathFilter.getOp().equals(Op.E)) {
          return new Pair<>(pathFilter.getPathA(), pathFilter.getPathB());
        }
        break;
      case Key:
      case Value:
      case Bool:
        break;
      default:
        throw new RuntimeException("Unexpected filter type: " + filter.getType());
    }
    return null;
  }

  public static void checkHeadersComparable(Header headerA, Header headerB)
      throws PhysicalException {
    // 检查是否同时有或没有key列
    if (headerA.hasKey() ^ headerB.hasKey()) {
      throw new InvalidOperatorParameterException(
          "Row stream to be union, except or intersect must have key or have not key at the same time.");
    }

    // 检查fields数量是否相等
    if (headerA.getFieldSize() != headerB.getFieldSize()) {
      throw new InvalidOperatorParameterException(
          "Row stream to be union, except or intersect must have the same number of fields.");
    }

    // 没有key列时，fields不能为空
    if (!headerA.hasKey() && headerA.getFieldSize() < 1) {
      throw new InvalidOperatorParameterException(
          "Row stream with no key to be union, except or intersect must have more than one field.");
    }

    // 检查对应位置的field是否可比较
    int size = headerA.getFieldSize();
    DataType typeA, typeB;
    for (int index = 0; index < size; index++) {
      typeA = headerA.getField(index).getType();
      typeB = headerB.getField(index).getType();
      boolean comparable = isNumericType(typeA) && isNumericType(typeB) || typeA.equals(typeB);
      if (!comparable) {
        throw new InvalidOperatorParameterException(
            "Field "
                + headerA.getField(index).getName()
                + "("
                + typeA
                + ") and field "
                + headerB.getField(index).getName()
                + "("
                + typeB
                + ") are incomparable.");
      }
    }
  }
}
