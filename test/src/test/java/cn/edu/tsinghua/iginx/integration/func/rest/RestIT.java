package cn.edu.tsinghua.iginx.integration.func.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.tool.ConfLoder;
import cn.edu.tsinghua.iginx.integration.tool.DBConf;
import cn.edu.tsinghua.iginx.rest.MetricsResource;
import cn.edu.tsinghua.iginx.session.Session;
import java.io.*;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestIT {
    protected Boolean ifClearData = true;

    protected static Logger logger = LoggerFactory.getLogger(MetricsResource.class);

    protected static Session session;
    protected Boolean isAbleToDelete = true;

    public RestIT() throws IOException {
        ConfLoder conf = new ConfLoder(Controller.CONFIG_FILE);
        DBConf dbConf = conf.loadDBConf(conf.getStorageType());
        this.ifClearData = dbConf.getEnumValue(DBConf.DBConfType.isAbleToClearData);
        this.isAbleToDelete = dbConf.getEnumValue(DBConf.DBConfType.isAbleToDelete);
    }

    @BeforeClass
    public static void setUp() throws SessionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        session.openSession();
    }

    @AfterClass
    public static void tearDown() throws SessionException {
        session.closeSession();
    }

    @Before
    public void insertData() {
        try {
            execute("insert.json", TYPE.INSERT);
        } catch (Exception e) {
            logger.error("insertData fail. Caused by: {}.", e.toString());
            fail();
        }
    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        Controller.clearData(session);
    }

    private enum TYPE {
        QUERY,
        INSERT,
        DELETE,
        DELETEMETRIC
    }

    public String orderGen(String json, TYPE type) {
        String ret = new String();
        if (type.equals(TYPE.DELETEMETRIC)) {
            ret = "curl -XDELETE";
            ret += " http://127.0.0.1:6666/api/v1/metric/{" + json + "}";
        } else {
            String prefix = "curl -XPOST -H\"Content-Type: application/json\" -d @";
            ret = prefix + json;
            if (type.equals(TYPE.QUERY)) ret += " http://127.0.0.1:6666/api/v1/datapoints/query";
            else if (type.equals(TYPE.INSERT)) ret += " http://127.0.0.1:6666/api/v1/datapoints";
            else if (type.equals(TYPE.DELETE))
                ret += " http://127.0.0.1:6666/api/v1/datapoints/delete";
        }
        return ret;
    }

    public String execute(String json, TYPE type) throws Exception {
        String ret = new String();
        String curlArray = orderGen(json, type);
        Process process = null;
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(curlArray.split(" "));
            processBuilder.directory(new File("./src/test/resources/restIT"));
            // 执行 url 命令
            process = processBuilder.start();

            // 输出子进程信息
            InputStreamReader inputStreamReaderINFO =
                    new InputStreamReader(process.getInputStream());
            BufferedReader bufferedReaderINFO = new BufferedReader(inputStreamReaderINFO);
            String lineStr;
            while ((lineStr = bufferedReaderINFO.readLine()) != null) {
                ret += lineStr;
            }
            // 等待子进程结束
            process.waitFor();

            return ret;
        } catch (InterruptedException e) {
            // 强制关闭子进程（如果打开程序，需要额外关闭）
            process.destroyForcibly();
            return null;
        }
    }

    public void executeAndCompare(String json, String output) {
        String result = new String();
        try {
            result = execute(json, TYPE.QUERY);
        } catch (Exception e) {
            //            if (e.toString().equals())
            logger.error("executeAndCompare fail. Caused by: {}.", e.toString());
        }
        assertEquals(output, result);
    }

    @Test
    public void testQueryWithoutTags() throws Exception {
        String json = "testQueryWithoutTags.json";
        String result =
                "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]},{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.search\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"host\": [\"server2\"]}, \"values\": [[1359786400000,321.0]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryWithTags() {
        String json = "testQueryWithTags.json";
        String result =
                "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788400000,123.3],[1359788410000,23.1]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryWrongTags() {
        String json = "testQueryWrongTags.json";
        String result =
                "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryOneTagWrong() {
        String json = "testQueryOneTagWrong.json";
        String result =
                "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryWrongName() {
        String json = "testQueryWrongName.json";
        String result =
                "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive_\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryWrongTime() {
        String json = "testQueryWrongTime.json";
        String result =
                "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        executeAndCompare(json, result);
    }

    //    @Test
    //    public void testQuery(){
    //        String json = "";
    //        String result = "";
    //        executeAndCompare(json,result);
    //    }

    @Test
    public void testQueryAvg() {
        String json = "testQueryAvg.json";
        String result =
                "{\"queries\":[{\"sample_size\": 3,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788298001,13.2],[1359788398001,123.3],[1359788408001,23.1]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryCount() {
        String json = "testQueryCount.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,3]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryFirst() {
        String json = "testQueryFirst.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryLast() {
        String json = "testQueryLast.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,23.1]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryMax() {
        String json = "testQueryMax.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,123.3]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQueryMin() {
        String json = "testQueryMin.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,13.2]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testQuerySum() {
        String json = "testQuerySum.json";
        String result =
                "{\"queries\":[{\"sample_size\": 1,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359763200001,159.6]]}]}]}";
        executeAndCompare(json, result);
    }

    @Test
    public void testDelete() throws Exception {
        if (!isAbleToDelete) return;
        String json = "testDelete.json";
        execute(json, TYPE.DELETE);

        String result =
                "{\"queries\":[{\"sample_size\": 2,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {\"data_center\": [\"DC1\"],\"host\": [\"server1\"]}, \"values\": [[1359788300000,13.2],[1359788410000,23.1]]}]}]}";
        json = "testQueryWithTags.json";
        executeAndCompare(json, result);
    }

    @Test
    public void testDeleteMetric() throws Exception {
        if (!isAbleToDelete) return;
        String json = "archive.file.tracked";
        execute(json, TYPE.DELETEMETRIC);

        String result =
                "{\"queries\":[{\"sample_size\": 0,\"results\": [{ \"name\": \"archive.file.tracked\",\"group_by\": [{\"name\": \"type\",\"type\": \"number\"}], \"tags\": {}, \"values\": []}]}]}";
        json = "testQueryWithTags.json";
        executeAndCompare(json, result);
    }

    @Test
    //    @Ignore
    public void pathVaildTest() throws Exception {
        try {
            String res = execute("pathVaildTest.json", TYPE.INSERT);
            logger.error("insertData fail. Caused by: {}.", res);
        } catch (Exception e) {
            logger.error("insertData fail. Caused by: {}.", e.toString());
        }
    }
}
