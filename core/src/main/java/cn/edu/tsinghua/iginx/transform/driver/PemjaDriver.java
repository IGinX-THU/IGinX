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
package cn.edu.tsinghua.iginx.transform.driver;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.TransformTaskMeta;
import cn.edu.tsinghua.iginx.transform.api.Writer;
import cn.edu.tsinghua.iginx.transform.pojo.PythonTask;
import java.io.File;
import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

public class PemjaDriver {

  private final IMetaManager metaManager = DefaultMetaManager.getInstance();

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String PATH =
      String.join(File.separator, config.getDefaultUDFDir(), "python_scripts");

  private static final String PY_SUFFIX = ".py";

  private static PemjaDriver instance;

  private PemjaDriver() {}

  public static PemjaDriver getInstance() {
    if (instance == null) {
      synchronized (PemjaDriver.class) {
        if (instance == null) {
          instance = new PemjaDriver();
        }
      }
    }
    return instance;
  }

  public PemjaWorker createWorker(PythonTask task, Writer writer) {
    String identifier = task.getPyTaskName();
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

    PythonInterpreter interpreter = new PythonInterpreter(config);
    String fileName = taskMeta.getFileName();
    String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
    String className = taskMeta.getClassName();

    // init the python udf
    interpreter.exec(String.format("import %s", moduleName));
    interpreter.exec(String.format("t = %s.%s()", moduleName, className));

    return new PemjaWorker(identifier, interpreter, writer);
  }
}
