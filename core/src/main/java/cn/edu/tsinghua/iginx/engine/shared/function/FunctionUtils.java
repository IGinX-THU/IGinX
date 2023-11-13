package cn.edu.tsinghua.iginx.engine.shared.function;

import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class FunctionUtils {

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
}
