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
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import static cn.edu.tsinghua.iginx.utils.ShellRunner.runCommand;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.system.ArithmeticExpr;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Avg;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Count;
import cn.edu.tsinghua.iginx.engine.shared.function.system.First;
import cn.edu.tsinghua.iginx.engine.shared.function.system.FirstValue;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.function.system.LastValue;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Min;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Ratio;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Sum;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDTF;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class FunctionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManager.class);

  private static final int INTERPRETER_NUM = 5;

  private final Map<String, Function> functions;

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PY_SUFFIX = ".py";

  private static final String PATH =
      String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  private FunctionManager() {
    this.functions = new HashMap<>();
    this.initSystemFunctions();
    if (config.isNeedInitBasicUDFFunctions()) {
      this.initBasicUDFFunctions();
    }
  }

  public static FunctionManager getInstance() {
    return FunctionManagerHolder.INSTANCE;
  }

  private void initSystemFunctions() {
    registerFunction(Avg.getInstance());
    registerFunction(Count.getInstance());
    registerFunction(FirstValue.getInstance());
    registerFunction(LastValue.getInstance());
    registerFunction(First.getInstance());
    registerFunction(Last.getInstance());
    registerFunction(Max.getInstance());
    registerFunction(Min.getInstance());
    registerFunction(Sum.getInstance());
    registerFunction(ArithmeticExpr.getInstance());
    registerFunction(Ratio.getInstance());
  }

  private void initBasicUDFFunctions() {
    List<TransformTaskMeta> metaList = new ArrayList<>();
    List<String> udfList = config.getUdfList();
    for (String udf : udfList) {
      String[] udfInfo = udf.split(",");
      if (udfInfo.length != 4) {
        LOGGER.error("udf info len must be 4.");
        continue;
      }
      UDFType udfType;
      switch (udfInfo[0].toLowerCase().trim()) {
        case "udaf":
          udfType = UDFType.UDAF;
          break;
        case "udtf":
          udfType = UDFType.UDTF;
          break;
        case "udsf":
          udfType = UDFType.UDSF;
          break;
        case "transform":
          udfType = UDFType.TRANSFORM;
          break;
        default:
          LOGGER.error("unknown udf type: {}", udfInfo[0]);
          continue;
      }
      metaList.add(
          new TransformTaskMeta(
              udfInfo[1],
              udfInfo[2],
              udfInfo[3],
              new HashSet<>(Collections.singletonList(config.getIp())),
              udfType));
    }

    for (TransformTaskMeta meta : metaList) {
      TransformTaskMeta taskMeta = metaManager.getTransformTask(meta.getName());
      if (taskMeta == null) {
        metaManager.addTransformTask(meta);
      } else if (!taskMeta.getIpSet().contains(config.getIp())) {
        meta.addIp(config.getIp());
        metaManager.updateTransformTask(meta);
      }

      if (!meta.getType().equals(UDFType.TRANSFORM)) {
        loadUDF(meta.getName());
      }
    }
  }

  public void registerFunction(Function function) {
    if (functions.containsKey(function.getIdentifier())) {
      return;
    }
    functions.put(function.getIdentifier(), function);
  }

  public Collection<Function> getFunctions() {
    return functions.values();
  }

  public Function getFunction(String identifier) {
    if (FunctionUtils.isSysFunc(identifier)) {
      identifier = identifier.toLowerCase();
    }
    if (functions.containsKey(identifier)) {
      return functions.get(identifier);
    }
    return loadUDF(identifier);
  }

  public void removeFunction(String identifier) {
    functions.remove(identifier);
  }

  private Function loadUDF(String identifier) {
    // load the udf & put it in cache.
    TransformTaskMeta taskMeta = metaManager.getTransformTask(identifier);
    if (taskMeta == null) {
      throw new IllegalArgumentException(String.format("UDF %s not registered", identifier));
    }
    if (!taskMeta.getIpSet().contains(config.getIp())) {
      throw new IllegalArgumentException(
          String.format("UDF %s not registered in node ip=%s", identifier, config.getIp()));
    }

    String pythonCMD = config.getPythonCMD();
    PythonInterpreterConfig config =
        PythonInterpreterConfig.newBuilder().setPythonExec(pythonCMD).addPythonPaths(PATH).build();

    String fileName = taskMeta.getFileName();
    String moduleName;
    String className = taskMeta.getClassName();
    if (fileName.endsWith(PY_SUFFIX)) {
      // accessing a python code file
      moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
      className = taskMeta.getClassName();
    } else {
      // accessing a python module dir
      moduleName = className.substring(0, className.lastIndexOf("."));
      className = className.substring(className.lastIndexOf(".") + 1);
    }

    // init the python udf
    BlockingQueue<PythonInterpreter> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < INTERPRETER_NUM; i++) {
      PythonInterpreter interpreter = new PythonInterpreter(config);
      interpreter.exec(String.format("import %s", moduleName));
      interpreter.exec(String.format("t = %s.%s()", moduleName, className));
      queue.add(interpreter);
    }

    if (taskMeta.getType().equals(UDFType.UDAF)) {
      PyUDAF udaf = new PyUDAF(queue, identifier);
      functions.put(identifier, udaf);
      return udaf;
    } else if (taskMeta.getType().equals(UDFType.UDTF)) {
      PyUDTF udtf = new PyUDTF(queue, identifier);
      functions.put(identifier, udtf);
      return udtf;
    } else if (taskMeta.getType().equals(UDFType.UDSF)) {
      PyUDSF udsf = new PyUDSF(queue, identifier);
      functions.put(identifier, udsf);
      return udsf;
    } else {
      while (!queue.isEmpty()) {
        queue.poll().close();
      }
      throw new IllegalArgumentException(
          String.format("UDF %s registered in type %s", identifier, taskMeta.getType()));
    }
  }

  // use pip to install requirements.txt in module root dir
  public void installReqsByPip(String rootPath) throws Exception {
    String reqFilePath = String.join(File.separator, PATH, rootPath, "requirements.txt");
    File file = new File(reqFilePath);
    if (file.exists()) {
      runCommand(config.getPythonCMD(), "-m", "pip", "install", "-r", reqFilePath);
    } else {
      LOGGER.warn("No requirement document provided for python module {}.", rootPath);
    }
  }

  public boolean hasFunction(String identifier) {
    return functions.containsKey(identifier);
  }

  private static class FunctionManagerHolder {

    private static final FunctionManager INSTANCE = new FunctionManager();

    private FunctionManagerHolder() {}
  }
}
