/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.shared.function;

import static cn.edu.tsinghua.iginx.utils.DataTypeUtils.isWholeNumber;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDTF;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FunctionUtils {

  private static final String PATH = "path";

  private static final String VALUE = "value";

  private static final Set<String> sysRowToRowFunctionSet =
      new HashSet<>(Collections.singletonList("ratio"));

  private static final Set<String> sysSetToRowFunctionSet =
      new HashSet<>(
          Arrays.asList("min", "max", "sum", "avg", "count", "first_value", "last_value"));

  private static final Set<String> sysSetToSetFunctionSet =
      new HashSet<>(Arrays.asList("first", "last"));

  private static final Set<String> sysCanUseSetQuantifierFunctionSet =
      new HashSet<>(Arrays.asList("min", "max", "sum", "avg", "count"));

  private static FunctionManager functionManager;

  private static void initFunctionManager() {
    if (functionManager != null) {
      return;
    }
    functionManager = FunctionManager.getInstance();
  }

  public static boolean isRowToRowFunction(String identifier) {
    if (sysRowToRowFunctionSet.contains(identifier.toLowerCase())) {
      return true;
    }
    initFunctionManager();
    Function function = functionManager.getFunction(identifier);
    return function.getIdentifier().equals("py_udtf");
  }

  public static boolean isSetToRowFunction(String identifier) {
    if (sysSetToRowFunctionSet.contains(identifier.toLowerCase())) {
      return true;
    }
    initFunctionManager();
    Function function = functionManager.getFunction(identifier);
    return function.getIdentifier().equals("py_udaf");
  }

  public static boolean isSetToSetFunction(String identifier) {
    if (sysSetToSetFunctionSet.contains(identifier.toLowerCase())) {
      return true;
    }
    initFunctionManager();
    Function function = functionManager.getFunction(identifier);
    return function.getIdentifier().equals("py_udsf");
  }

  public static boolean isCanUseSetQuantifierFunction(String identifier) {
    return sysCanUseSetQuantifierFunctionSet.contains(identifier.toLowerCase());
  }

  public static boolean isSysFunc(String identifier) {
    identifier = identifier.toLowerCase();
    return sysRowToRowFunctionSet.contains(identifier)
        || sysSetToRowFunctionSet.contains(identifier)
        || sysSetToSetFunctionSet.contains(identifier);
  }

  public static boolean isPyUDF(String identifier) {
    if (sysRowToRowFunctionSet.contains(identifier.toLowerCase())
        || sysSetToRowFunctionSet.contains(identifier.toLowerCase())
        || sysSetToSetFunctionSet.contains(identifier.toLowerCase())) {
      return false;
    }
    initFunctionManager();
    Function function = functionManager.getFunction(identifier);
    return function.getIdentifier().equals("py_udtf")
        || function.getIdentifier().equals("py_udaf")
        || function.getIdentifier().equals("py_udsf");
  }

  public static String getFunctionName(Function function) {
    if (function.getIdentifier().equals("py_udtf")) {
      return ((PyUDTF) function).getFunctionName();
    } else if (function.getIdentifier().equals("py_udaf")) {
      return ((PyUDAF) function).getFunctionName();
    } else if (function.getIdentifier().equals("py_udsf")) {
      return ((PyUDSF) function).getFunctionName();
    } else {
      return function.getIdentifier();
    }
  }

  static Map<String, java.util.function.Function<DataType, DataType>> functionFieldTypeMap =
      new HashMap<>(); // 此Map用于存储MappingFunction的输出类型

  static {
    functionFieldTypeMap.put("avg", (dataType) -> DataType.DOUBLE);
    functionFieldTypeMap.put("count", (dataType) -> DataType.LONG);
    functionFieldTypeMap.put("first_value", (dataType) -> dataType);
    functionFieldTypeMap.put("last_value", (dataType) -> dataType);
    functionFieldTypeMap.put("max", (dataType) -> dataType);
    functionFieldTypeMap.put("min", (dataType) -> dataType);
    functionFieldTypeMap.put(
        "sum", (dataType) -> isWholeNumber(dataType) ? DataType.LONG : DataType.DOUBLE);
  }

  /**
   * 用于提取table中的表头字段和对应的索引
   *
   * @param table 输入的表
   * @param params 函数参数
   * @param function 函数本身，用于命名输出列
   * @return Pair，第一个元素是输出的表头字段，第二个元素是对应的索引
   */
  public static Pair<List<Field>, List<Integer>> getFieldAndIndices(
      Table table, FunctionParams params, SetMappingFunction function) {
    List<Field> fields = table.getHeader().getFields();
    List<String> pathParams = params.getPaths();

    if (pathParams == null || pathParams.size() != 1) {
      throw new IllegalArgumentException(
          String.format("unexpected param type for %s.", function.getIdentifier()));
    }

    String target = pathParams.get(0);

    Pattern pattern = Pattern.compile(StringUtils.reformatPath(target) + ".*");
    List<Field> targetFields = new ArrayList<>();
    List<Integer> indices = new ArrayList<>();
    for (int i = 0; i < fields.size(); i++) {
      Field field = fields.get(i);
      if (pattern.matcher(field.getFullName()).matches()) {
        String name = function.getIdentifier() + "(";
        String fullName = function.getIdentifier() + "(";
        if (params.isDistinct()) {
          name += "distinct ";
          fullName += "distinct ";
        }
        name += field.getName() + ")";
        fullName += field.getFullName() + ")";
        DataType dataType =
            functionFieldTypeMap.get(function.getIdentifier()).apply(field.getType());
        targetFields.add(new Field(name, fullName, dataType));
        indices.add(i);
      }
    }
    return new Pair<>(targetFields, indices);
  }

  /**
   * 提取了MappingFunction中的First和Last的公共部分, 用于计算First和Last
   *
   * @param table 输入的表
   * @param params 函数参数
   * @param function 函数本身，用于判断是First还是Last，逻辑有细微差别
   * @return First或者Last的结果
   */
  public static RowStream firstOrLastTransform(
      Table table, FunctionParams params, MappingFunction function) {
    List<String> pathParams = params.getPaths();
    if (pathParams == null || pathParams.size() != 1) {
      throw new IllegalArgumentException(
          String.format("unexpected param type for %s.", function.getIdentifier()));
    }

    String target = pathParams.get(0);
    Header header =
        new Header(
            Field.KEY,
            Arrays.asList(new Field(PATH, DataType.BINARY), new Field(VALUE, DataType.BINARY)));
    List<Row> resultRows = new ArrayList<>();
    Map<Integer, Pair<Long, Object>> valueMap = new HashMap<>();
    Pattern pattern = Pattern.compile(StringUtils.reformatPath(target) + ".*");
    Set<Integer> indices = new HashSet<>();
    for (int i = 0; i < table.getHeader().getFieldSize(); i++) {
      Field field = table.getHeader().getField(i);
      if (pattern.matcher(field.getFullName()).matches()) {
        indices.add(i);
      }
    }

    boolean isLast = Objects.equals(function.getIdentifier(), Last.LAST);
    boolean isFirst = Objects.equals(function.getIdentifier(), First.FIRST);

    for (Row row : table.getRows()) {
      if (valueMap.size() >= indices.size() && isFirst) {
        break;
      }
      Object[] values = row.getValues();

      for (int i = 0; i < values.length; i++) {
        if (values[i] == null || !indices.contains(i)) {
          continue;
        }
        if (!valueMap.containsKey(i)) {
          valueMap.put(i, new Pair<>(row.getKey(), values[i]));
        } else if (isLast) {
          Pair<Long, Object> pair = valueMap.get(i);
          pair.k = row.getKey();
          pair.v = values[i];
        }
      }
    }

    for (Map.Entry<Integer, Pair<Long, Object>> entry : valueMap.entrySet()) {
      resultRows.add(
          new Row(
              header,
              entry.getValue().k,
              new Object[] {
                table
                    .getHeader()
                    .getField(entry.getKey())
                    .getFullName()
                    .getBytes(StandardCharsets.UTF_8),
                ValueUtils.toString(
                        entry.getValue().v, table.getHeader().getField(entry.getKey()).getType())
                    .getBytes(StandardCharsets.UTF_8)
              }));
    }
    resultRows.sort(ValueUtils.firstLastRowComparator());
    return new Table(header, resultRows);
  }

  public static List<String> getFunctionsFullPath(List<FunctionCall> functionCalls) {
    return functionCalls.stream().map(FunctionCall::getFunctionStr).collect(Collectors.toList());
  }

  public static List<String> getFunctionsFullPath(Operator operator) {
    if (operator.getType() == OperatorType.Downsample) {
      return getFunctionsFullPath(((Downsample) operator).getFunctionCallList());
    } else if (operator.getType() == OperatorType.GroupBy) {
      return getFunctionsFullPath(((GroupBy) operator).getFunctionCallList());
    } else if (operator.getType() == OperatorType.SetTransform) {
      return getFunctionsFullPath(((SetTransform) operator).getFunctionCallList());
    } else if (operator.getType() == OperatorType.MappingTransform) {
      return getFunctionsFullPath(((MappingTransform) operator).getFunctionCallList());
    } else if (operator.getType() == OperatorType.RowTransform) {
      return getFunctionsFullPath(((RowTransform) operator).getFunctionCallList());
    } else {
      return new ArrayList<>();
    }
  }
}
