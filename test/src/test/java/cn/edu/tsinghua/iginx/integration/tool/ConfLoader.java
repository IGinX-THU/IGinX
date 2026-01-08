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
package cn.edu.tsinghua.iginx.integration.tool;

import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.FileReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfLoader.class);

  private static final String STORAGE_ENGINE_LIST = "storageEngineList";

  private static final String RELATIONAL_STORAGE_ENGINE_LIST = "relationalStorageEngineList";

  private static final String RELATIONAL = "Relational";

  private static final String TEST_LIST = "test-list";

  private static final String DBCONF = "%s-config";

  private static final String DB_MOCK_CONF = "%s_mock";

  private static final String DB_CLASS_NAME = "%s_class";

  private static final String DB_HISTORY_DATA_GEN_CLASS_NAME = "%s_data_gen_class";

  private static final String DB_PORT_MAP = "%s_port";

  private static final String RUNNING_STORAGE = "./src/test/resources/DBName.txt";

  private static final String IS_SCALING = "./src/test/resources/isScaling.txt";

  private static final String DBCE_TEST_WAY = "./src/test/resources/dbce-test-way.txt";

  private static final String DEFAULT_DB = "stand_alone_DB";

  private static final String DEFAULT_IS_SCALING = "is_scaling";

  private static final String DEFAULT_DBCE_TEST_WAY = "DBCE_test_way";

  private static List<String> storageEngines = new ArrayList<>();

  private Map<StorageEngineType, List<String>> taskMap = new HashMap<>();

  private static String confPath;

  private static Properties properties = null;

  private boolean DEBUG = false;

  private void logInfo(String info, Object... args) {
    if (DEBUG) {
      LOGGER.info(info, args);
    }
  }

  private String getTestProperty(String path, String defaultProperty) {
    String result = null;
    File file = new File(path);
    if (file.exists()) {
      result = FileReader.convertToString(path);
    } else {
      logInfo(path + "does not exist and use default storage type");
      result = properties.getProperty(defaultProperty);
      logInfo("default property: {}", result);
    }
    return result;
  }

  public String getStorageType() {
    return getTestProperty(RUNNING_STORAGE, DEFAULT_DB);
  }

  public String getStorageType(boolean needSpecific) {
    String storageType = getStorageType();
    logInfo("run the test on {}", storageType);
    if (needSpecific) {
      return storageType;
    }

    try {
      InputStream in = Files.newInputStream(Paths.get(confPath));
      Properties properties = new Properties();
      properties.load(in);
      if (Arrays.asList(properties.getProperty(RELATIONAL_STORAGE_ENGINE_LIST).split(","))
          .contains(storageType)) {
        storageType = RELATIONAL;
      }
    } catch (IOException e) {
      LOGGER.error("load conf failure: ", e);
    }
    return storageType;
  }

  public boolean isScaling() {
    return Boolean.parseBoolean(getTestProperty(IS_SCALING, DEFAULT_IS_SCALING));
  }

  public String getDBCETestWay() {
    return getTestProperty(DBCE_TEST_WAY, DEFAULT_DBCE_TEST_WAY);
  }

  public List<String> getQueryIds() {
    String queryIds = properties.getProperty("query_ids");
    queryIds = properties.getProperty(String.format("%s_query_ids", getStorageType()), queryIds);
    return Arrays.asList(queryIds.split(","));
  }

  public int getMaxRepetitionsNum() {
    return Integer.parseInt(properties.getProperty("max_repetitions_num"));
  }

  public double getRegressionThreshold() {
    return Double.parseDouble(properties.getProperty("regression_threshold"));
  }

  public ConfLoader(String confPath) {
    this.confPath = confPath;
    try {
      if (properties == null) {
        InputStream in = Files.newInputStream(Paths.get(confPath));
        properties = new Properties();
        properties.load(in);
      }
    } catch (IOException e) {
      LOGGER.error("load conf failure: ", e);
    }
  }

  public void loadTestConf() throws IOException {
    logInfo("loading the test conf...");
    String property = properties.getProperty(STORAGE_ENGINE_LIST);
    if (property == null || property.isEmpty()) {
      return;
    }
    storageEngines = Arrays.asList(property.split(","));

    // load the task list
    for (String storageEngine : storageEngines) {
      String tasks;
      String storage = storageEngine.toLowerCase();
      tasks = properties.getProperty(storage + "-" + TEST_LIST);
      if (tasks == null) {
        tasks = properties.getProperty(TEST_LIST);
      }
      logInfo("the task of {} is :", storageEngine);
      List<String> oriTaskList = Arrays.asList(tasks.split(",")), taskList = new ArrayList<>();
      for (String taskName : oriTaskList) {
        if (taskName.contains("{}")) {
          taskName = taskName.replace("{}", storageEngine);
        }
        taskList.add(taskName);
        logInfo("taskName: {}", taskName);
      }
      taskMap.put(StorageEngineType.valueOf(storageEngine.toLowerCase()), taskList);
    }
  }

  public DBConf loadDBConf(String storageEngine) {
    DBConf dbConf = new DBConf();
    logInfo("loading the DB conf...");
    String property = properties.getProperty(STORAGE_ENGINE_LIST);
    if (property == null || property.isEmpty()) {
      return dbConf;
    }
    storageEngines = Arrays.asList(property.split(","));

    if (storageEngine == null || storageEngine.isEmpty()) {
      return dbConf;
    }
    String confs = properties.getProperty(String.format(DBCONF, storageEngine));
    logInfo("the conf of {} is : {}", storageEngine, confs);
    String[] confList = confs.split(",");
    for (String conf : confList) {
      String[] confKV = conf.split("=");
      dbConf.setEnumValue(DBConf.getDBConfType(confKV[0]), Boolean.parseBoolean(confKV[1]));
    }
    dbConf.setStorageEngineMockConf(
        properties.getProperty(String.format(DB_MOCK_CONF, storageEngine)));
    dbConf.setClassName(properties.getProperty(String.format(DB_CLASS_NAME, storageEngine)));
    dbConf.setHistoryDataGenClassName(
        properties.getProperty(String.format(DB_HISTORY_DATA_GEN_CLASS_NAME, storageEngine)));

    // dbce port map
    String portMap = properties.getProperty(String.format(DB_PORT_MAP, storageEngine));
    logInfo("the db port map of {} is : {}", storageEngine, portMap);
    String[] ports = portMap.split(",");
    for (int i = 0; i < ports.length; i++) {
      String port = ports[i];
      int portNum = Integer.parseInt(port);
      switch (i) {
        case 0:
          dbConf.setDbcePortMap(Constant.ORI_PORT_NAME, portNum);
          break;
        case 1:
          dbConf.setDbcePortMap(Constant.EXP_PORT_NAME, portNum);
          break;
        case 2:
          dbConf.setDbcePortMap(Constant.READ_ONLY_PORT_NAME, portNum);
          break;
      }
    }
    return dbConf;
  }

  public Map<StorageEngineType, List<String>> getTaskMap() {
    return taskMap;
  }
}
