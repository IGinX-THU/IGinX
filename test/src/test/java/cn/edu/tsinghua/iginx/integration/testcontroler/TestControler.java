package cn.edu.tsinghua.iginx.integration.testcontroler;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.integration.tool.TestConfLoder;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.utils.ShellRunner;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public class TestControler {
    protected static final Logger logger = LoggerFactory.getLogger(TestControler.class);
    public static String CLEARDATAEXCP = "cn.edu.tsinghua.iginx.exceptions.ExecutionException: Caution: can not clear the data of read-only node.";
    public static String CONFIG_FILE = "./src/test/java/cn/edu/tsinghua/iginx/integration/testcontroler/testConfig.properties";
    private String FILEPATH = "./src/test/resources/testTask.txt";
    private String MVNRUNTEST = "../.github/testUnion.sh";
    private List<StorageEngineMeta> storageEngineMetas = new ArrayList<>();


    public static void clearData(Session session) throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = null;
        try {
            res = session.executeSql(clearData);
        } catch (SessionException | ExecutionException e) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}", clearData, e.toString());
            if (e.toString().equals(CLEARDATAEXCP) || e.toString().equals("\n" + CLEARDATAEXCP)) {
                logger.error("clear data fail and go on....");
            }
            else fail();
        }

        if (res != null && res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Statement: \"{}\" execute fail. Caused by: {}.", clearData, res.getParseErrorMsg());
            fail();
        }
    }

    @Test
    public void testUnion() throws Exception {
        // load the test conf
        TestConfLoder testConfLoder = new TestConfLoder(CONFIG_FILE);
        testConfLoder.loadTestConf();
        storageEngineMetas = testConfLoder.getStorageEngineMetas();

        ShellRunner shellRunner = new ShellRunner();
        TestEnvironmentControler envir = new TestEnvironmentControler();

        // ori plan
//        // skip this when support remove Engine
//        shellRunner.runShellCommand(MVNRUNTEST);
//        // for each storage , run the test
//        for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
//            // add the storage engine
//            envir.addStorageEngine(storageEngineMeta);
//            // set the task list
//            envir.setTestTasks(testConfLoder.getTaskMap().get(storageEngineMeta.getStorageEngine()), FILEPATH);
//            // run the test together
//            shellRunner.runShellCommand(MVNRUNTEST);
//        }

        // set the task list
        envir.setTestTasks(testConfLoder.getTaskMap().get(DBConf.getDBType(testConfLoder.getStorageType())), FILEPATH);
        // run the test together
        shellRunner.runShellCommand(MVNRUNTEST);
    }
}