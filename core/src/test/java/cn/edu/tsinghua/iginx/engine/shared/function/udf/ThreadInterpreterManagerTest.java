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
package cn.edu.tsinghua.iginx.engine.shared.function.udf;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.ThreadInterpreterManager;
import cn.edu.tsinghua.iginx.utils.EnvUtils;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pemja.core.PythonInterpreterConfig;

public class ThreadInterpreterManagerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadInterpreterManagerTest.class);

  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final List<String> PATHS =
      Arrays.asList(
          Paths.get(
                  EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir")),
                  "src",
                  "test",
                  "resources",
                  "udf")
              .toString(),
          Paths.get(
                  EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir")),
                  "..",
                  "udf_funcs",
                  "python_scripts")
              .toString(),
          Paths.get(
                  EnvUtils.loadEnv(Constants.IGINX_HOME, System.getProperty("user.dir")),
                  "..",
                  "udf_funcs",
                  "python_scripts",
                  "utils")
              .toString());

  private static final String PYTHON_CMD = config.getPythonCMD();

  @BeforeClass
  public static void setUp() throws Exception {
    PythonInterpreterConfig.PythonInterpreterConfigBuilder builder =
        PythonInterpreterConfig.newBuilder().setPythonExec(PYTHON_CMD);
    PATHS.forEach(builder::addPythonPaths);
    PythonInterpreterConfig config = builder.build();
    ThreadInterpreterManager.setConfig(config);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    ThreadInterpreterManager.getInterpreter().close();
  }

  @Test
  public void invokeMethodWithTimeoutTest() {
    String moduleName = "timeout_test", obj = "t", className = "TimeoutTest";
    ThreadInterpreterManager.exec(
        String.format("import %s; %s = %s.%s()", moduleName, obj, moduleName, className));
    try {
      Object res =
          ThreadInterpreterManager.invokeMethodWithTimeout(
              3, obj, "timeout", new ArrayList<>(), new ArrayList<>(), new HashMap<>());
    } catch (Exception e) {
      if (e.getMessage().contains("timeout")) {
        LOGGER.info("Successfully detected timeout and terminated the thread.");
      } else {
        LOGGER.error("Failed to detect timeout and terminated the thread.", e);
      }
    } finally {
      ThreadInterpreterManager.getInterpreter().close();
    }

    try {
      Object res =
          ThreadInterpreterManager.invokeMethodWithTimeout(
              3, obj, "waitForEvent", new ArrayList<>(), new ArrayList<>(), new HashMap<>());
    } catch (Exception e) {
      if (e.getMessage().contains("timeout")) {
        LOGGER.info("Successfully detected timeout and terminated the thread.");
      } else {
        LOGGER.error(e.getMessage());
        LOGGER.error("Failed to detect timeout and terminated the thread.", e);
      }
    } finally {
      ThreadInterpreterManager.getInterpreter().close();
    }

    try {
      Object res =
          ThreadInterpreterManager.invokeMethodWithTimeout(
              1, obj, "downloadLargeModel", new ArrayList<>(), new ArrayList<>(), new HashMap<>());
    } catch (Exception e) {
      if (e.getMessage().contains("timeout")) {
        LOGGER.info("Successfully detected timeout and terminated the thread.");
      } else {
        LOGGER.error(e.getMessage());
        LOGGER.error("Failed to detect timeout and terminated the thread.", e);
      }
    } finally {
      ThreadInterpreterManager.getInterpreter().close();
    }
  }
}
