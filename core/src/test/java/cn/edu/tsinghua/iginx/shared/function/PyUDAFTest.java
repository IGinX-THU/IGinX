package cn.edu.tsinghua.iginx.shared.function;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_CLASS;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.UDF_FUNC;

import java.util.*;
import org.junit.Test;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class PyUDAFTest {

  @Test
  public void test() {
    List<List<Object>> data = new ArrayList<>();
    data.add(
        new ArrayList<>(
            Arrays.asList(
                "label", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1",
                "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1",
                "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1", "col_1",
                "col_1", "col_1", "col_1")));
    data.add(
        new ArrayList<>(
            Arrays.asList(
                "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE",
                "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE")));
    for (int i = 0; i < 3000000; i++) {
      List<Object> rowData = new ArrayList<>();
      for (int j = 0; j < 30; j++) {
        rowData.add(Math.random());
      }
      data.add(rowData);
    }
    List<Object> args = new ArrayList<>();
    Map<String, Object> kvargs = new HashMap<>();

    String pythonCMD = "/opt/homebrew/anaconda3/envs/iginx/bin/python";
    PythonInterpreterConfig config =
        PythonInterpreterConfig.newBuilder()
            .setPythonExec(pythonCMD)
            .addPythonPaths("/Users/janet/Desktop/IGinX/udf_funcs/python_scripts")
            .build();
    PythonInterpreter interpreter = new PythonInterpreter(config);

    // interpreter.exec("import iginx_udf");
    interpreter.exec("import udaf_test");
    interpreter.exec("t = udaf_test.UDAFtest()");

    List<List<Object>> res =
        (List<List<Object>>) interpreter.invokeMethod(UDF_CLASS, UDF_FUNC, data, args, kvargs);
    System.out.println(res);
  }
}
