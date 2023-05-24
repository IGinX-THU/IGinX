package cn.edu.tsinghua.iginx.integration.expansion;

import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.unit.SQLTestTools;
import cn.edu.tsinghua.iginx.pool.SessionPool;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class CapacityExpansionIT implements BaseCapacityExpansionIT {
    private static final Logger logger = LoggerFactory.getLogger(CapacityExpansionIT.class);
    protected static Session session;
    protected static SessionPool sessionPool;
    protected String ENGINE_TYPE;

    public CapacityExpansionIT(String engineType) {
        this.ENGINE_TYPE = engineType;
    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        Controller.clearData(session);
    }

    @BeforeClass
    public static void setUp() throws SessionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        sessionPool =
                new SessionPool.Builder()
                        .host("127.0.0.1")
                        .port(6888)
                        .user("root")
                        .password("root")
                        .maxSize(3)
                        .build();
        session.openSession();
    }

    @AfterClass
    public static void tearDown() throws SessionException {
        session.closeSession();
        sessionPool.close();
    }

    @Test
    public void oriHasDataExpHasData() throws Exception {
        testQueryHistoryDataFromInitialNode();
        testQueryAfterInsertNewData();
        testOriHasDataExpHasData();
        testWriteAndQueryAfterCEOriHasDataExpHasData();
    }

    @Test
    public void oriHasDataExpNoData() throws Exception {
        testQueryHistoryDataFromInitialNode();
        testQueryAfterInsertNewData();
        testOriHasDataExpNoData();
        testWriteAndQueryAfterCEOriHasDataExpNoData();
    }

    @Test
    public void oriNoDataExpHasData() throws Exception {
        testQueryHistoryDataFromNoInitialNode();
        testQueryAfterInsertNewDataFromNoInitialNode();
        testOriNoDataExpHasData();
        testWriteAndQueryAfterCEOriNoDataExpHasData();
    }

    @Test
    public void oriNoDataExpNoData() throws Exception {
        testQueryHistoryDataFromNoInitialNode();
        testQueryAfterInsertNewDataFromNoInitialNode();
        testOriNoDataExpNoData();
        testWriteAndQueryAfterCEOriNoDataExpNoData();
    }

    protected abstract void addStorageWithPrefix(String dataPrefix, String schemaPrefix)
            throws Exception;

    protected abstract int getPort() throws Exception;

    protected boolean queryHistoryDataA(Session session) throws Exception {
        String statement = "select * from mn";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|key|mn.wf01.wt01.status|mn.wf01.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|100|               true|                    null|\n"
                        + "|200|              false|                   20.71|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from mn.wf01";
        expect =
                "ResultSets:\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|count(mn.wf01.wt01.status)|count(mn.wf01.wt01.temperature)|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|                         2|                              1|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        return true;
    }

    protected boolean queryHistoryDataB(Session session) throws Exception {
        String statement = "select * from mn.wf03";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|key|mn.wf03.wt01.status|mn.wf03.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "| 77|               true|                    null|\n"
                        + "|200|              false|                   77.71|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        return true;
    }

    protected boolean queryAllHistoryData(Session session) throws Exception {
        String statement = "select * from mn";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+\n"
                        + "|key|mn.wf01.wt01.status|mn.wf01.wt01.temperature|mn.wf03.wt01.status|mn.wf03.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+\n"
                        + "| 77|               null|                    null|               true|                    null|\n"
                        + "|100|               true|                    null|               null|                    null|\n"
                        + "|200|              false|                   20.71|              false|                   77.71|\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+\n"
                        + "Total line number = 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        return true;
    }

    @Test
    public void testPrefixAndRemoveHistoryDataSource() throws Exception {
        addStorageWithPrefix("mn", "p1");
        addStorageWithPrefix("mn", "p2");
        String statement = "select * from p1.mn";
        String expect =
                "ResultSets:\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "|key|p1.mn.wf03.wt01.status|p1.mn.wf03.wt01.temperature|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "| 77|                  true|                       null|\n"
                        + "|200|                 false|                      77.71|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from p2.mn";
        expect =
                "ResultSets:\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "|key|p2.mn.wf03.wt01.status|p2.mn.wf03.wt01.temperature|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "| 77|                  true|                       null|\n"
                        + "|200|                 false|                      77.71|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
        removedStorageEngineList.add(
                new RemovedStorageEngineInfo("127.0.0.1", getPort(), "p2", "mn"));
        sessionPool.removeHistoryDataSource(removedStorageEngineList);
        statement = "select * from p2.mn";
        expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        session.executeSql(
                "remove historydataresource (\"127.0.0.1\", " + getPort() + ", \"p1\", \"mn\")");
        statement = "select * from p1.mn";
        expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void addStorageEngine(boolean hasData) throws SessionException, ExecutionException {
        if (ENGINE_TYPE.toLowerCase().contains("iotdb"))
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 6668, \""
                            + ENGINE_TYPE
                            + "\", \"username:root, password:root, sessionPoolSize:20, has_data:"
                            + hasData
                            + ", is_read_only:true\");");
        else if (ENGINE_TYPE.toLowerCase().contains("influxdb"))
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 8087, \""
                            + ENGINE_TYPE
                            + "\", \"url:http://localhost:8087/, username:user, password:12345678, sessionPoolSize:20, has_data:"
                            + hasData
                            + ", is_read_only:true, token:testToken, organization:testOrg\");");
        else if (ENGINE_TYPE.toLowerCase().contains("parquet"))
            session.executeSql(
                    "ADD STORAGEENGINE (\"127.0.0.1\", 6668, \""
                            + ENGINE_TYPE
                            + "\", \"username:root, password:root, sessionPoolSize:20, has_data:"
                            + hasData
                            + ", is_read_only:true\");");
        else {
            logger.error("not support the DB: {}", ENGINE_TYPE);
            fail();
        }
    }

    // @Test
    public void testQueryHistoryDataFromInitialNode() throws Exception {
        queryHistoryDataA(session);
    }

    public void testQueryHistoryDataFromNoInitialNode() throws Exception {
        String statement = "select * from mn";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    // @Test
    public void testQueryAfterInsertNewData() throws Exception {
        testQueryAfterInsertNewDataFromNoInitialNode();
        queryHistoryDataA(session);
    }

    public void queryNewDataA(Session session) throws Exception {
        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+---+--------------+---------------+\n"
                        + "|key|ln.wf02.status|ln.wf02.version|\n"
                        + "+---+--------------+---------------+\n"
                        + "|100|          true|             v1|\n"
                        + "|400|         false|             v4|\n"
                        + "|800|          null|             v8|\n"
                        + "+---+--------------+---------------+\n"
                        + "Total line number = 3\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect =
                "ResultSets:\n"
                        + "+---------------------+----------------------+\n"
                        + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
                        + "+---------------------+----------------------+\n"
                        + "|                    2|                     3|\n"
                        + "+---------------------+----------------------+\n"
                        + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testQueryAfterInsertNewDataFromNoInitialNode() throws Exception {
        session.executeSql(
                "insert into ln.wf02 (key, status, version) values (100, true, \"v1\");");
        session.executeSql(
                "insert into ln.wf02 (key, status, version) values (400, false, \"v4\");");
        session.executeSql("insert into ln.wf02 (key, version) values (800, \"v8\");");

        queryNewDataA(session);
    }

    // @Test
    public void testOriHasDataExpNoData() throws Exception {
        addStorageEngine(false);
        String statement = "select * from mn.wf03";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        queryNewDataA(session);
        queryHistoryDataA(session);
    }

    public void testOriHasDataExpHasData() throws Exception {
        addStorageEngine(true);
        queryNewDataA(session);
        queryAllHistoryData(session);
    }

    public void testOriNoDataExpHasData() throws Exception {
        addStorageEngine(true);
        queryHistoryDataB(session);
        queryNewDataA(session);
    }

    public void testOriNoDataExpNoData() throws Exception {
        addStorageEngine(false);
        String statement = "select * from mn.wf03";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
        queryNewDataA(session);
    }

    public void queryAllNewData(Session session) throws Exception {
        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+----+--------------+---------------+\n"
                        + "| key|ln.wf02.status|ln.wf02.version|\n"
                        + "+----+--------------+---------------+\n"
                        + "| 100|          true|             v1|\n"
                        + "| 400|         false|             v4|\n"
                        + "| 800|          null|             v8|\n"
                        + "|1600|          null|            v48|\n"
                        + "+----+--------------+---------------+\n"
                        + "Total line number = 4\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf02";
        expect =
                "ResultSets:\n"
                        + "+---------------------+----------------------+\n"
                        + "|count(ln.wf02.status)|count(ln.wf02.version)|\n"
                        + "+---------------------+----------------------+\n"
                        + "|                    2|                     4|\n"
                        + "+---------------------+----------------------+\n"
                        + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    // @Test
    public void testWriteAndQueryAfterCEOriHasDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
        queryAllHistoryData(session);
        queryAllNewData(session);
    }

    public void testWriteAndQueryAfterCEOriNoDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
        queryAllNewData(session);
        queryHistoryDataB(session);
    }

    public void testWriteAndQueryAfterCEOriHasDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
        queryAllNewData(session);
        queryHistoryDataA(session);
    }

    public void testWriteAndQueryAfterCEOriNoDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
        queryAllNewData(session);
    }
}
