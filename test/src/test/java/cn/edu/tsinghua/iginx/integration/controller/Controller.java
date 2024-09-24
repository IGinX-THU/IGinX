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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package cn.edu.tsinghua.iginx.integration.controller;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.expPort;
import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.oriPort;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exception.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.expansion.constant.Constant;
import cn.edu.tsinghua.iginx.integration.func.session.InsertAPIType;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.util.*;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

  private static final Logger LOGGER = LoggerFactory.getLogger(Controller.class);

  public static final String CLEAR_DATA = "CLEAR DATA;";

  public static final String CLEAR_DATA_WARNING = "clear data fail and go on...";

  public static final String CONFIG_FILE = "./src/test/resources/testConfig.properties";

  private static final String TEST_TASK_FILE = "./src/test/resources/testTask.txt";

  private static final String MVN_RUN_TEST = "../.github/scripts/test/test_union.sh";

  // 将数据划分为两部分，一部分写入dummy数据库，一部分写入非dummy数据库, 0.3 为划分比例，即 30% 的数据写入 dummy 数据库
  private static final double PARTITION_POINT = 0.3;
  // 向 dummy 分片写入的初始化序列，用来初始化 dummy 分片的原数据空间范围
  public static final String DUMMY_INIT_PATH_BEGIN = "b.b.b";
  public static final String DUMMY_INIT_PATH_END =
      "zzzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzz.zzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
  private static final String EXP_HAS_DATA_STRING = "ExpHasData";
  private static final String ORI_HAS_DATA_STRING = "oriHasData";
  private static final ConfLoader testConf = new ConfLoader(Controller.CONFIG_FILE);

  public static final Map<String, Boolean> SUPPORT_KEY =
      new HashMap<String, Boolean>() {
        {
          put(
              testConf.getStorageType(),
              testConf
                  .loadDBConf(testConf.getStorageType())
                  .getEnumValue(DBConf.DBConfType.isSupportKey));
        }
      };

  public static final Map<String, Boolean> NEED_SEPARATE_WRITING =
      new HashMap<String, Boolean>() {
        {
          put(
              testConf.getStorageType(),
              testConf
                  .loadDBConf(testConf.getStorageType())
                  .getEnumValue(DBConf.DBConfType.isSupportDiffTypeHistoryData));
        }
      };

  public static void clearAllData(Session session) {
    clearAllData(new MultiConnection(session));
  }

  public static void clearAllData(MultiConnection session) {
    clearData(session);
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    if (!conf.isScaling()) {
      LOGGER.info("Not the DBCE test, skip the clear history data step.");
    } else {
      BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
      if (generator == null) {
        LOGGER.error("clear data fail, caused by generator is null");
        return;
      }
      generator.clearHistoryData();
    }
  }

  public static void clearData(Session session) {
    clearData(new MultiConnection(session));
  }

  public static void clearData(MultiConnection session) {
    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(CLEAR_DATA);
    } catch (SessionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
        LOGGER.warn(CLEAR_DATA_WARNING);
      } else {
        LOGGER.error("Statement: \"{}\" execute fail. Caused by: ", CLEAR_DATA, e);
        fail();
      }
    }

    if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      LOGGER.error(
          "Statement: \"{}\" execute fail. Caused by: {}", CLEAR_DATA, res.getParseErrorMsg());
      fail();
    }
  }

  private static BaseHistoryDataGenerator getCurrentGenerator(ConfLoader conf) {
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    String instance = dbConf.getHistoryDataGenClassName();
    try {
      return (BaseHistoryDataGenerator) Class.forName(instance).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      LOGGER.error("write data fail, caused by: ", e);
      fail();
    }
    return null;
  }

  public static <T> void writeColumnsData(
      T session,
      List<String> pathList,
      List<List<Long>> keyList,
      List<DataType> dataTypeList,
      List<List<Object>> valuesList,
      List<Map<String, String>> tagsList,
      InsertAPIType type,
      boolean needWriteHistoryData) {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    // medium 为划分数据的分界点，即前 medium 个数据写入非 dummy 数据库，后 medium 个数据写入 dummy 数据库
    int medium = 0;
    if (!conf.isScaling() || !NEED_SEPARATE_WRITING.get(conf.getStorageType())) {
      LOGGER.info("skip the write history data step.");
      medium = pathList.size();
    } else {
      LOGGER.info("DBCE test, write history data.");
      boolean IS_EXP_DUMMY = testConf.getDBCETestWay().contains(EXP_HAS_DATA_STRING);
      boolean IS_ORI_DUMMY = testConf.getDBCETestWay().contains(ORI_HAS_DATA_STRING);
      medium =
          (tagsList == null
                      || tagsList.isEmpty()
                      || tagsList.stream().allMatch(map -> map.size() == 0))
                  && (IS_EXP_DUMMY || IS_ORI_DUMMY)
              ? (int) (pathList.size() * PARTITION_POINT)
              : pathList.size();
    }

    for (int i = 0; i < pathList.size(); i++) {
      if (i <= medium) {
        try { // write data through session
          Object[] value = valuesList.get(i).toArray();
          Object[] valueToInsert = new Object[1];
          valueToInsert[0] = value;
          writeDataWithSession(
              session,
              Collections.singletonList(pathList.get(i)),
              keyList.get(i).stream().mapToLong(Long::longValue).toArray(),
              valueToInsert,
              Collections.singletonList(dataTypeList.get(i)),
              Collections.singletonList(tagsList.get(i)),
              type);
        } catch (SessionException e) {
          LOGGER.error("write data fail, caused by: ", e);
          fail();
        }
      } else {
        if (!needWriteHistoryData) {
          break;
        }
        // 需要对4种情况做区分的情况
        boolean IS_EXP_DUMMY = testConf.getDBCETestWay().contains(EXP_HAS_DATA_STRING);
        boolean IS_ORI_DUMMY = testConf.getDBCETestWay().contains(ORI_HAS_DATA_STRING);
        int port;
        // 如果是has,has情况，则dummy数据的一半写入ori数据库，另一半写入exp数据库
        if (i < (1 + 1 / PARTITION_POINT) / 2 * medium) {
          port = IS_EXP_DUMMY ? expPort : oriPort;
        } else {
          port = IS_ORI_DUMMY ? oriPort : expPort;
        }
        List<List<Object>> rowValues = convertColumnsToRows(valuesList.get(i));
        BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
        if (generator == null) {
          LOGGER.error("write data fail, caused by generator is null");
          return;
        }
        generator.writeHistoryData(
            port,
            Collections.singletonList(pathList.get(i)),
            Collections.singletonList(dataTypeList.get(i)),
            keyList.get(i),
            rowValues);
      }
    }
  }

  public static <T> void writeRowsDataToDummy(
      T session,
      List<String> pathList,
      List<Long> keyList,
      List<DataType> dataTypeList,
      List<List<Object>> valuesList,
      int port) {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    BaseHistoryDataGenerator generator = getCurrentGenerator(conf);
    if (generator == null) {
      LOGGER.error("write data fail, caused by generator is null");
      return;
    }
    generator.writeHistoryData(port, pathList, dataTypeList, keyList, valuesList);
  }

  public static <T> void writeRowsData(
      T session,
      List<String> pathList,
      List<Long> keyList,
      List<DataType> dataTypeList,
      List<List<Object>> valuesList,
      List<Map<String, String>> tagsList,
      InsertAPIType type,
      boolean needWriteHistoryData) {
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    DBConf dbConf = conf.loadDBConf(conf.getStorageType());
    Constant.oriPort = dbConf.getDBCEPortMap().get(Constant.ORI_PORT_NAME);
    Constant.expPort = dbConf.getDBCEPortMap().get(Constant.EXP_PORT_NAME);
    // medium 为划分数据的分界点，即前 medium 个数据写入非 dummy 数据库，后 medium 个数据写入 dummy 数据库
    int medium = 0;
    if (!conf.isScaling() || !NEED_SEPARATE_WRITING.get(conf.getStorageType())) {
      LOGGER.info("skip the write history data step.");
      medium = keyList.size();
    } else {
      LOGGER.info("DBCE test, write history data.");
      boolean IS_EXP_DUMMY = testConf.getDBCETestWay().contains(EXP_HAS_DATA_STRING);
      boolean IS_ORI_DUMMY = testConf.getDBCETestWay().contains(ORI_HAS_DATA_STRING);
      medium =
          (tagsList == null
                      || tagsList.isEmpty()
                      || tagsList.stream().allMatch(map -> map.size() == 0))
                  && (IS_EXP_DUMMY || IS_ORI_DUMMY)
              ? (int) (keyList.size() * PARTITION_POINT)
              : keyList.size();
    }

    // divide the data
    List<Long> upperkeyList = keyList;
    List<Long> lowerKeyList = null;
    List<List<Object>> upperValuesList = valuesList;
    List<List<Object>> lowerValuesList = null;
    // 划分数据区间
    if (medium != keyList.size() && medium != 0) {
      upperkeyList = keyList.subList(0, medium); // 上半部分，包括索引为 0 到 midIndex-1 的元素
      lowerKeyList =
          keyList.subList(medium, keyList.size()); // 下半部分，包括索引为 midIndex 到 keyList.size()-1 的元素
      upperValuesList = valuesList.subList(0, medium);
      lowerValuesList = valuesList.subList(medium, keyList.size());
    }

    // transfer the List to Array
    Object[] newValuesList = new Object[upperkeyList.size()];
    for (int j = 0; j < upperkeyList.size(); j++) {
      Object[] value = new Object[pathList.size()];
      for (int k = 0; k < pathList.size(); k++) {

        value[k] = upperValuesList.get(j).get(k);
      }
      newValuesList[j] = value;
    }

    try { // write data through session
      writeDataWithSession(
          session,
          pathList,
          upperkeyList.stream().mapToLong(Long::longValue).toArray(),
          newValuesList,
          dataTypeList,
          tagsList,
          type);
    } catch (SessionException e) {
      LOGGER.error("write data fail, caused by: ", e);
      fail();
    }

    if (lowerKeyList != null && !lowerKeyList.isEmpty() && needWriteHistoryData) {
      // 需要对4种情况做区分的情况
      boolean IS_EXP_DUMMY = testConf.getDBCETestWay().contains(EXP_HAS_DATA_STRING);
      boolean IS_ORI_DUMMY = testConf.getDBCETestWay().contains(ORI_HAS_DATA_STRING);
      int port;
      if (IS_EXP_DUMMY && IS_ORI_DUMMY) {
        // divide the data
        List<Long> upperDummyKeyList = null;
        List<Long> lowerDummyKeyList = null;
        List<List<Object>> upperDummyValuesList = null;
        List<List<Object>> lowerDummyValuesList = null;
        int half = lowerKeyList.size() / 2;
        // 划分数据区间
        upperDummyKeyList = lowerKeyList.subList(0, half); // 上半部分，包括索引为 0 到 half-1 的元素
        lowerDummyKeyList =
            lowerKeyList.subList(
                half, lowerKeyList.size()); // 下半部分，包括索引为 half 到 lowerKeyList.size()-1 的元素
        upperDummyValuesList = lowerValuesList.subList(0, half);
        lowerDummyValuesList = lowerValuesList.subList(half, lowerKeyList.size());
        writeRowsDataToDummy(
            session, pathList, upperDummyKeyList, dataTypeList, upperDummyValuesList, oriPort);
        writeRowsDataToDummy(
            session, pathList, lowerDummyKeyList, dataTypeList, lowerDummyValuesList, expPort);
      } else {
        // 如果是has,has情况，则dummy数据的一半写入ori数据库，另一半写入exp数据库
        port = IS_EXP_DUMMY ? expPort : oriPort;
        writeRowsDataToDummy(session, pathList, lowerKeyList, dataTypeList, lowerValuesList, port);
      }
    }
  }

  // 处理IT在每个写入数据后先关操作
  public static <T> void after(T session) {
    // do nothing
  }

  private static <T> void writeDataWithSession(
      T session,
      List<String> paths,
      long[] timestamps,
      Object[] valuesList,
      List<DataType> dataTypeList,
      List<Map<String, String>> tagsList,
      InsertAPIType type)
      throws SessionException {
    MultiConnection multiConnection = null;
    if (session instanceof MultiConnection) {
      multiConnection = ((MultiConnection) session);
    } else if (session instanceof Session) {
      multiConnection = new MultiConnection(((Session) session));
    }
    switch (type) {
      case Row:
        multiConnection.insertRowRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case NonAlignedRow:
        multiConnection.insertNonAlignedRowRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case Column:
        multiConnection.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
      case NonAlignedColumn:
        multiConnection.insertNonAlignedColumnRecords(
            paths, timestamps, valuesList, dataTypeList, tagsList);
        break;
    }
  }

  public static <T> List<List<T>> convertColumnsToRows(List<T> inputList) {
    List<List<T>> outputList = new ArrayList<>();

    for (T item : inputList) {
      List<T> subList = new ArrayList<>();
      subList.add(item);
      outputList.add(subList);
    }

    return outputList;
  }

  @Test
  public void testUnion() throws Exception {
    // load the test conf
    ConfLoader testConfLoader = new ConfLoader(CONFIG_FILE);
    testConfLoader.loadTestConf();

    ShellRunner shellRunner = new ShellRunner();
    TestEnvironmentController envir = new TestEnvironmentController();

    // set the task list
    envir.setTestTasks(
        testConfLoader
            .getTaskMap()
            .get(StorageEngineType.valueOf(testConfLoader.getStorageType(false).toLowerCase())),
        TEST_TASK_FILE);
    // run the test together
    shellRunner.runShellCommand(MVN_RUN_TEST);
  }
}
