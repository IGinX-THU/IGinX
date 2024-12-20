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
package cn.edu.tsinghua.iginx.transform.driver;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
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

  protected static final String PY_SUFFIX = ".py";

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

  public static PythonInterpreterConfig getPythonConfig() {
    String pythonCMD = config.getPythonCMD();
    return PythonInterpreterConfig.newBuilder()
        .setPythonExec(pythonCMD)
        .addPythonPaths(PATH)
        .build();
  }

  public PemjaWorker createWorker(PythonTask task, Writer writer) {
    String identifier = task.getPyTaskName();
    TransformTaskMeta taskMeta = metaManager.getTransformTask(identifier);
    if (taskMeta == null) {
      throw new IllegalArgumentException(String.format("UDF %s not registered", identifier));
    }
    if (!taskMeta.containsIpPort(config.getIp(), config.getPort())) {
      throw new IllegalArgumentException(
          String.format("UDF %s not registered in node ip=%s", identifier, config.getIp()));
    }

    PythonInterpreter interpreter = ThreadInterpreterManager.getInterpreter();
    String fileName = taskMeta.getFileName();
    String moduleName = fileName.substring(0, fileName.indexOf(PY_SUFFIX));
    String className = taskMeta.getClassName();

    // to fail fast
    // importlib is used to update python scripts
    interpreter.exec(String.format("import %s; import importlib", moduleName));

    return new PemjaWorker(identifier, moduleName, className, interpreter, writer);
  }
}
