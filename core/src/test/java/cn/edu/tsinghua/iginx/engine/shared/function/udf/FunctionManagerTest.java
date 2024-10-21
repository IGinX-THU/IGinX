package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import org.junit.Test;
import pemja.core.PythonInterpreter;

public class FunctionManagerTest {
  private static final FunctionManager functionManager = FunctionManager.getInstance();

  private static final PythonInterpreter interpreter = functionManager.getInterpreter();

  public FunctionManagerTest() {}

  @Test
  public void removeModuleTest() throws Exception {
    interpreter.exec("import math");
    functionManager.removePythonModule("math");

    interpreter.exec("print(math.sqrt(4))");
  }
}
