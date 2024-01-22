package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

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
        targetFields.add(new Field(name, fullName, field.getType()));
        indices.add(i);
      }
    }
    return new Pair<>(targetFields, indices);
  }

  public static RowStream firstOrLastTransform(
      Table rows, FunctionParams params, MappingFunction function) throws Exception {
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
    for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
      Field field = rows.getHeader().getField(i);
      if (pattern.matcher(field.getFullName()).matches()) {
        indices.add(i);
      }
    }

    boolean isFirst = function instanceof First;

    for (Row row : rows.getRows()) {
      if (valueMap.size() >= indices.size()) {
        break;
      }
      Object[] values = row.getValues();

      for (int i = 0; i < values.length; i++) {
        if (values[i] == null || !indices.contains(i)) {
          continue;
        }
        if (!valueMap.containsKey(i)) {
          valueMap.put(i, new Pair<>(row.getKey(), values[i]));
        } else if (!isFirst) {
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
                rows.getHeader()
                    .getField(entry.getKey())
                    .getFullName()
                    .getBytes(StandardCharsets.UTF_8),
                ValueUtils.toString(
                        entry.getValue().v, rows.getHeader().getField(entry.getKey()).getType())
                    .getBytes(StandardCharsets.UTF_8)
              }));
    }
    resultRows.sort(ValueUtils.firstLastRowComparator());
    return new Table(header, resultRows);
  }
}
