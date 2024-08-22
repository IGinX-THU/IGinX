package cn.edu.tsinghua.iginx.engine.shared.function.udf.python;

import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import java.util.concurrent.BlockingQueue;
import pemja.core.PythonInterpreter;

public abstract class PyUDF implements Function {

  protected final BlockingQueue<PythonInterpreter> interpreters;

  protected final String moduleName;

  public PyUDF(BlockingQueue<PythonInterpreter> interpreters, String moduleName) {
    this.interpreters = interpreters;
    this.moduleName = moduleName;
  }

  public void close() {
    while (!interpreters.isEmpty()) {
      PythonInterpreter interpreter = interpreters.poll();
      if (interpreter != null) {
        // remove the module
        interpreter.exec(String.format("import sys; sys.modules.pop('%s', None)", moduleName));
        interpreter.close();
      }
    }
  }
}
