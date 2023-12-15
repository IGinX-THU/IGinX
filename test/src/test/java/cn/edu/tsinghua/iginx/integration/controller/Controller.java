package cn.edu.tsinghua.iginx.integration.controller;

import static cn.edu.tsinghua.iginx.constant.GlobalConstant.CLEAR_DUMMY_DATA_CAUTION;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoader;
import cn.edu.tsinghua.iginx.integration.tool.MultiConnection;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Controller {

  private static final Logger logger = LoggerFactory.getLogger(Controller.class);

  public static final String CLEAR_DATA = "CLEAR DATA;";

  public static final String CLEAR_DATA_WARNING = "clear data fail and go on...";

  public static final String CLEAR_DATA_ERROR = "Statement: \"{}\" execute fail. Caused by: {}";

  public static final String CONFIG_FILE = "./src/test/resources/testConfig.properties";

  private static final String TEST_TASK_FILE = "./src/test/resources/testTask.txt";

  private static final String MVN_RUN_TEST = "../.github/scripts/test/test_union.sh";

  public static final Map<String, Boolean> SUPPORT_KEY =
      new HashMap<String, Boolean>() {
        {
          put("filesystem", false);
          put("iotdb12", true);
          put("influxdb", true);
          put("postgresql", false);
          put("redis", false);
          put("mongodb", false);
          put("parquet", true);
        }
      };

  private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();

  public static void clearData(Session session) {
    clearData(new MultiConnection(session));
  }

  public static void clearData(MultiConnection session) {
    SessionExecuteSqlResult res = null;
    try {
      res = session.executeSql(CLEAR_DATA);
    } catch (SessionException | ExecutionException e) {
      if (e.toString().trim().contains(CLEAR_DUMMY_DATA_CAUTION)) {
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
