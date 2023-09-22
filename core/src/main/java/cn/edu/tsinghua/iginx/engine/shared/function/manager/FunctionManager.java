/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
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

  private static final int INTERPRETER_NUM = 5;

  private final Map<String, Function> functions;

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final Logger logger = LoggerFactory.getLogger(FunctionManager.class);

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
        logger.error("udf info len must be 4.");
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
          logger.error("unknown udf type: " + udfInfo[0]);
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
    if (functions.containsKey(identifier)) {
      return functions.get(identifier);
    }
    return loadUDF(identifier);
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
    String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
    String className = taskMeta.getClassName();

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

  public boolean hasFunction(String identifier) {
    return functions.containsKey(identifier);
  }

  private static class FunctionManagerHolder {

    private static final FunctionManager INSTANCE = new FunctionManager();

    private FunctionManagerHolder() {}
  }
}
