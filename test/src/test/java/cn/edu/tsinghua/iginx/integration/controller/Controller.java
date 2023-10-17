package cn.edu.tsinghua.iginx.integration.controller;

import static cn.edu.tsinghua.iginx.integration.expansion.constant.Constant.expPort;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.expansion.BaseHistoryDataGenerator;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
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

  private static final Logger logger = LoggerFactory.getLogger(Controller.class);

  public static final String CLEAR_DATA_EXCEPTION =
      "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";

  public static final String CLEAR_DATA = "CLEAR DATA;";

  public static final String CLEAR_DATA_WARNING = "clear data fail and go on...";

  public static final String CLEAR_DATA_ERROR = "Statement: \"{}\" execute fail. Caused by: {}";

  public static final String CONFIG_FILE = "./src/test/resources/testConfig.properties";

  private static final String TEST_TASK_FILE = "./src/test/resources/testTask.txt";

  private static final String MVN_RUN_TEST = "../.github/scripts/test/test_union.sh";

  private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

  private static final Map<String, String> NAME_TO_INSTANCE = new HashMap<String, String>()
  {
    {
      put("FileSystem", "cn.edu.tsinghua.iginx.integration.expansion.filesystem.FileSystemHistoryDataGenerator");
      put("IoTDB12", "cn.edu.tsinghua.iginx.integration.expansion.iotdb.IoTDB12HistoryDataGenerator");
      put("InfluxDB", "cn.edu.tsinghua.iginx.integration.expansion.influxdb.InfluxDBHistoryDataGenerator");
      put("PostgreSQL", "cn.edu.tsinghua.iginx.integration.expansion.postgresql.PostgreSQLHistoryDataGenerator");
      put("Redis", "cn.edu.tsinghua.iginx.integration.expansion.redis.RedisHistoryDataGenerator");
      put("MongoDB", "cn.edu.tsinghua.iginx.integration.expansion.mongodb.MongoDBHistoryDataGenerator");
      put("Parquet", "cn.edu.tsinghua.iginx.integration.expansion.parquet.ParquetHistoryDataGenerator");
    }
  };

  private static final Map<String, Boolean> SUPPORT_KEY = new HashMap<String, Boolean>()
  {
    {
      put("FileSystem", false);
      put("IoTDB12", true);
      put("InfluxDB", true);
      put("PostgreSQL", false);
      put("Redis", false);
      put("MongoDB", false);
      put("Parquet", true);
    }
  };

  public static void clearData(Session session) {
    clearData(new MultiConnection(session));
  }

  public static void clearData(MultiConnection session) {
    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(CLEAR_DATA);
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().equals(CLEAR_DATA_EXCEPTION)) {
        logger.warn(CLEAR_DATA_WARNING);
      } else {
        logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, e.getMessage());
        fail();
      }
    }

    if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
      logger.error(CLEAR_DATA_ERROR, CLEAR_DATA, res.getParseErrorMsg());
      fail();
    }
  }

  public static void writeColumnsData(Session session, List<String> pathList, List<List<Long>> keyList, List<DataType> dataTypeList, List<List<Object>> valuesList, List<Map<String, String>> tagsList) {
    String instance = null;
    ConfLoader conf = new ConfLoader(Controller.CONFIG_FILE);
    int medium = tagsList ==null ? pathList.size() / 2 : pathList.size();
    if (conf.getStorageType() == null) {
      logger.info("Not the DBCE test, skip the write data step.");
      medium = pathList.size();
    } else {
      instance = NAME_TO_INSTANCE.get(conf.getStorageType());
    }

    for (int i = 0; i < pathList.size(); i++) {
      if (i<=medium) {
        try { // write data through session
          session.insertColumnRecords(Collections.singletonList(pathList.get(i)),
              keyList.get(i).stream().mapToLong(Long::longValue).toArray(),
              valuesList.get(i).toArray(),
              Collections.singletonList(dataTypeList.get(i)),
              Collections.singletonList(tagsList.get(i)));
        } catch (SessionException | ExecutionException e) {
          logger.error("write data fail, caused by: {}", e.getMessage());
          fail();
        }
      }
      // write data to dummy storage, and should insert by row
      List<List<Object>> rowValues = convertColumnsToRows(valuesList.get(i));
      List<List<Long>> rowKeys = convertColumnsToRows(keyList.get(i));
      try {
        BaseHistoryDataGenerator generator = (BaseHistoryDataGenerator) Class.forName(instance).newInstance();
        generator.writeHistoryData(expPort, Collections.singletonList(pathList.get(i)), Collections.singletonList(dataTypeList.get(i)), rowKeys, rowValues);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        logger.error("write data fail, caused by: {}", e.getMessage());
        fail();
      }
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
            .get(StorageEngineType.valueOf(testConfLoader.getStorageType().toLowerCase())),
        TEST_TASK_FILE);
    // run the test together
    shellRunner.runShellCommand(MVN_RUN_TEST);
  }
}
