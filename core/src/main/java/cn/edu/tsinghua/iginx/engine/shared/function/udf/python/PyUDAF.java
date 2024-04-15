package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionType;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.UDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.DataUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.RowUtils;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
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
  public Row transform(Table table, FunctionParams params) throws Exception {
    if (!CheckUtils.isLegal(params)) {
      throw new IllegalArgumentException("unexpected params for PyUDAF.");
    }

    PythonInterpreter interpreter = interpreters.take();
    List<List<Object>> data = DataUtils.dataFromTable(table, params.getPaths());
    if (data == null) {
      return Row.EMPTY_ROW;
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
