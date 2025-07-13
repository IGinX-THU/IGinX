/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 * TSIGinX@gmail.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package cn.edu.tsinghua.iginx.engine.shared.function.manager;

import static cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.Constants.PYTHON_PATHS;
import static cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.Constants.SCRIPTS_PATH;
import static cn.edu.tsinghua.iginx.utils.ShellRunner.runCommand;
import static cn.edu.tsinghua.iginx.utils.ShellRunner.runCommandAndGetResult;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.Function;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionUtils;
import cn.edu.tsinghua.iginx.engine.shared.function.system.*;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDAF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDSF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.python.PyUDTF;
import cn.edu.tsinghua.iginx.engine.shared.function.udf.utils.CheckUtils;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.PyFunctionMeta;
import cn.edu.tsinghua.iginx.thrift.UDFType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class FunctionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManager.class);

  private final Map<String, Function> functions;

  private static final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PY_SUFFIX = ".py";

  private PythonInterpreterConfig INTERPRETER_CONFIG;

  private PythonInterpreter interpreter;

  private static final String PythonCMD = config.getPythonCMD();

  private FunctionManager() {
    this.functions = new HashMap<>();
    LOGGER.debug("main thread: using pythonCMD: {}", PythonCMD);
    if (LOGGER.isDebugEnabled()) {
      try {
        String sitePath =
            runCommandAndGetResult(
                "", PythonCMD, "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])");
        LOGGER.debug("main thread: python site path: {}", sitePath);
      } catch (Exception e) {
        LOGGER.debug("failed to get purelib path", e);
      }
    }
    this.initSystemFunctions();
    if (config.isNeedInitBasicUDFFunctions()) {
      this.initBasicUDFFunctions();
    }
  }

  public PythonInterpreterConfig getConfig() {
    if (INTERPRETER_CONFIG == null) {
      this.INTERPRETER_CONFIG =
          PythonInterpreterConfig.newBuilder()
              .setPythonExec(PythonCMD)
              .addPythonPaths(PYTHON_PATHS)
              .build();
    }
    return INTERPRETER_CONFIG;
  }

  public PythonInterpreter getInterpreter() {
    if (interpreter == null) {
      ThreadInterpreterManager.setConfig(getConfig());
      interpreter = ThreadInterpreterManager.getInterpreter();
    }
    return interpreter;
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
    registerFunction(Extract.getInstance());
    registerFunction(Ratio.getInstance());
    registerFunction(SubString.getInstance());
  }

  private void initBasicUDFFunctions() {
    List<PyFunctionMeta> metaList = new ArrayList<>();
    List<String> udfList = config.getUdfList();
    for (String udf : udfList) {
      LOGGER.debug("initing udf: {}", udf);
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
      LOGGER.debug(
          "adding udf : {}, {}, {}, {}, {}, {}",
          udfInfo[1],
          udfInfo[2],
          udfInfo[3],
          config.getIp(),
          config.getPort(),
          udfType);
      metaList.add(
          new PyFunctionMeta(
              udfInfo[1], udfInfo[2], udfInfo[3], config.getIp(), config.getPort(), udfType));
    }

    for (PyFunctionMeta meta : metaList) {
      LOGGER.debug("loading udf meta:{}", meta);
      PyFunctionMeta taskMeta = metaManager.getPyFunction(meta.getName());
      if (taskMeta == null) {
        metaManager.addPyFunction(meta);
      } else if (!taskMeta.containsIpPort(config.getIp(), config.getPort())) {
        meta.addIpPort(config.getIp(), config.getPort());
        metaManager.updatePyFunction(meta);
      }

      if (!meta.getType().equals(UDFType.TRANSFORM)) {
        LOGGER.debug("Loading UDF meta: {}", meta);
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
    if (functions.containsKey(identifier)) {
      PyUDF function = (PyUDF) functions.get(identifier);
      try {
        function.close(identifier, interpreter);
      } catch (Exception e) {
        LOGGER.error("Failed to remove UDF {}.", identifier, e);
      }
    }
    functions.remove(identifier);
  }

  private Function loadUDF(String identifier) {
    // load the udf & put it in cache.
    PyFunctionMeta functionMeta = metaManager.getPyFunction(identifier);
    if (functionMeta == null) {
      throw new IllegalArgumentException(String.format("UDF %s not registered", identifier));
    }
    if (!functionMeta.containsIpPort(config.getIp(), config.getPort())) {
      throw new IllegalArgumentException(
          String.format("UDF %s not registered in node ip=%s", identifier, config.getIp()));
    }

    String fileName = functionMeta.getFileName();
    String moduleName;
    String className = functionMeta.getClassName();
    if (fileName.endsWith(PY_SUFFIX)) {
      // accessing a python code file
      moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
      className = functionMeta.getClassName();
    } else {
      // accessing a python module dir
      moduleName = className.substring(0, className.lastIndexOf("."));
      className = className.substring(className.lastIndexOf(".") + 1);
    }

    if (functionMeta.getType().equals(UDFType.UDAF)) {
      PyUDAF udaf = new PyUDAF(identifier, moduleName, className);
      functions.put(identifier, udaf);
      return udaf;
    } else if (functionMeta.getType().equals(UDFType.UDTF)) {
      PyUDTF udtf = new PyUDTF(identifier, moduleName, className);
      functions.put(identifier, udtf);
      return udtf;
    } else if (functionMeta.getType().equals(UDFType.UDSF)) {
      PyUDSF udsf = new PyUDSF(identifier, moduleName, className);
      functions.put(identifier, udsf);
      return udsf;
    } else {
      throw new IllegalArgumentException(
          String.format("UDF %s registered in type %s", identifier, functionMeta.getType()));
    }
  }

  // use pip to install requirements.txt in module root dir
  public void installReqsByPip(String rootPath) throws Exception {
    String reqFilePath = String.join(File.separator, SCRIPTS_PATH, rootPath, "requirements.txt");
    File file = new File(reqFilePath);
    if (file.exists()) {
      runCommand(PythonCMD, "-m", "pip", "install", "-r", reqFilePath);
    } else {
      LOGGER.warn("No requirement document provided for python module {}.", rootPath);
    }
  }

  public Set<String> checkScript(File file) throws IOException {
    if (file.isFile()) {
      return CheckUtils.importCheck(file.getCanonicalPath());
    } else {
      Set<String> res = new HashSet<>();
      try (Stream<Path> stream = Files.walk(file.toPath())) {
        List<Path> list =
            stream
                .filter(Files::isRegularFile)
                .filter(path -> path.toString().endsWith(".py")) // py文件
                .collect(Collectors.toList());
        for (Path path : list) {
          Set<String> fileRes = CheckUtils.importCheck(path.toFile().getCanonicalPath());
          res.addAll(fileRes);
        }
      }
      return res;
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
