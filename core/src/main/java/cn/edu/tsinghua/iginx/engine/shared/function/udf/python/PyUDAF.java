package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Pattern;
import pemja.core.PythonInterpreter;

public class PyUDAF implements UDAF {

  private static final String PY_UDAF = "py_udaf";

  private final BlockingQueue<PythonInterpreter> interpreters;

  private final String funcName;

  public PyUDAF(BlockingQueue<PythonInterpreter> interpreter, String funcName) {
    this.interpreters = interpreter;
    this.funcName = funcName;
  }

  @Override
  public FunctionType getFunctionType() {
    return FunctionType.UDF;
  }

  @Override
  public MappingType getMappingType() {
    return MappingType.SetMapping;
  }

  @Override
  public String getIdentifier() {
    return PY_UDAF;
  }

  @Override
  public Row transform(RowStream rows, FunctionParams params) throws Exception {
    if (!CheckUtils.isLegal(params)) {
      throw new IllegalArgumentException("unexpected params for PyUDAF.");
    }

    PythonInterpreter interpreter = interpreters.take();

    List<Object> colNames = new ArrayList<>(Collections.singletonList("key"));
    List<Object> colTypes = new ArrayList<>(Collections.singletonList(DataType.LONG.toString()));
    List<Integer> indices = new ArrayList<>();

    List<String> paths = params.getPaths();
    flag:
    for (String target : paths) {
      if (StringUtils.isPattern(target)) {
        Pattern pattern = Pattern.compile(StringUtils.reformatPath(target));
        for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
          Field field = rows.getHeader().getField(i);
          if (pattern.matcher(field.getName()).matches()) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
          }
        }
      } else {
        for (int i = 0; i < rows.getHeader().getFieldSize(); i++) {
          Field field = rows.getHeader().getField(i);
          if (target.equals(field.getName())) {
            colNames.add(field.getName());
            colTypes.add(field.getType().toString());
            indices.add(i);
            continue flag;
          }
        }
      }
    }

    if (colNames.size() == 1) {
      return Row.EMPTY_ROW;
    }

    List<List<Object>> data = new ArrayList<>();
    data.add(colNames);
    data.add(colTypes);
    while (rows.hasNext()) {
      Row row = rows.next();
      List<Object> rowData = new ArrayList<>();
      rowData.add(row.getKey());
      for (Integer idx : indices) {
        rowData.add(row.getValues()[idx]);
      }
      data.add(rowData);
    }

    List<Object> args = params.getArgs();
    Map<String, Object> kvargs = params.getKwargs();

    List<List<Object>> res =
        (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs);

    if (res == null || res.size() < 3) {
      return Row.EMPTY_ROW;
    }
    interpreters.add(interpreter);

    // [["key", col1, col2 ....],
    // ["LONG", type1, type2 ...],
    // [key1, val11, val21 ...]]
    boolean hasKey = res.get(0).get(0).equals("key");
    long key = -1;
    if (hasKey) {
      res.get(0).remove(0);
      res.get(1).remove(0);
      key = (Long) res.get(2).remove(0);
    }

    Header header = RowUtils.constructHeaderWithFirstTwoRowsUsingFuncName(res, hasKey, funcName);
    return hasKey
        ? RowUtils.constructNewRowWithKey(header, key, res.get(2))
        : RowUtils.constructNewRow(header, res.get(2));
  }

  @Override
  public String getFunctionName() {
    return funcName;
  }
}
