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
    protected String dataPrefix = "ln";
    protected String schemaPrefix = "p1";

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

    @Test
    public void testPrefixAndRemoveHistoryDataSource() throws Exception {
        addStorageWithPrefix("ln", "p1");
        addStorageWithPrefix("ln", "p2");
        String statement = "select * from p1.ln";
        String expect =
                "ResultSets:\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "|key|p1.ln.wf03.wt01.status|p1.ln.wf03.wt01.temperature|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "| 77|                  true|                       null|\n"
                        + "|200|                 false|                      77.71|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from p2.ln";
        expect =
                "ResultSets:\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "|key|p2.ln.wf03.wt01.status|p2.ln.wf03.wt01.temperature|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "| 77|                  true|                       null|\n"
                        + "|200|                 false|                      77.71|\n"
                        + "+---+----------------------+---------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
        removedStorageEngineList.add(
                new RemovedStorageEngineInfo("127.0.0.1", getPort(), "p2", "ln"));
        sessionPool.removeHistoryDataSource(removedStorageEngineList);
        statement = "select * from p2.ln";
        expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        session.executeSql(
                "remove historydataresource (\"127.0.0.1\", " + getPort() + ", \"p1\", \"ln\")");
        statement = "select * from p1.ln";
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
        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|100|               true|                    null|\n"
                        + "|200|              false|                   20.71|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select count(*) from ln.wf01";
        expect =
                "ResultSets:\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|count(ln.wf01.wt01.status)|count(ln.wf01.wt01.temperature)|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|                         2|                              1|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testQueryHistoryDataFromNoInitialNode() throws Exception {
        String statement = "select * from ln";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    // @Test
    public void testQueryAfterInsertNewData() throws Exception {
        session.executeSql(
                "insert into ln.wf02 (key, status, version) values (100, true, \"v1\");");
        session.executeSql(
                "insert into ln.wf02 (key, status, version) values (400, false, \"v4\");");
        session.executeSql("insert into ln.wf02 (key, version) values (800, \"v8\");");

        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "|100|               true|                    null|          true|             v1|\n"
                        + "|200|              false|                   20.71|          null|           null|\n"
                        + "|400|               null|                    null|         false|             v4|\n"
                        + "|800|               null|                    null|          null|             v8|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "Total line number = 4\n";
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

    // @Test
    public void testOriHasDataExpNoData() throws Exception {
        addStorageEngine(false);
        String statement = "select * from ln.wf03";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "|100|               true|                    null|          true|             v1|\n"
                        + "|200|              false|                   20.71|          null|           null|\n"
                        + "|400|               null|                    null|         false|             v4|\n"
                        + "|800|               null|                    null|          null|             v8|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+\n"
                        + "Total line number = 4\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testOriHasDataExpHasData() throws Exception {
        addStorageEngine(true);
        String statement = "select * from ln.wf03";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|key|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "| 77|               true|                    null|\n"
                        + "|200|              false|                   77.71|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "| 77|               null|                    null|          null|           null|               true|                    null|\n"
                        + "|100|               true|                    null|          true|             v1|               null|                    null|\n"
                        + "|200|              false|                   20.71|          null|           null|              false|                   77.71|\n"
                        + "|400|               null|                    null|         false|             v4|               null|                    null|\n"
                        + "|800|               null|                    null|          null|             v8|               null|                    null|\n"
                        + "+---+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testOriNoDataExpHasData() throws Exception {
        addStorageEngine(true);
        String statement = "select * from ln.wf03";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+\n"
                        + "|key|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "| 77|               true|                    null|\n"
                        + "|200|              false|                   77.71|\n"
                        + "+---+-------------------+------------------------+\n"
                        + "Total line number = 2\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect =
                "ResultSets:\n"
                        + "+---+--------------+---------------+-------------------+------------------------+\n"
                        + "|key|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+---+--------------+---------------+-------------------+------------------------+\n"
                        + "| 77|          null|           null|               true|                    null|\n"
                        + "|100|          true|             v1|               null|                    null|\n"
                        + "|200|          null|           null|              false|                   77.71|\n"
                        + "|400|         false|             v4|               null|                    null|\n"
                        + "|800|          null|             v8|               null|                    null|\n"
                        + "+---+--------------+---------------+-------------------+------------------------+\n"
                        + "Total line number = 5\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testOriNoDataExpNoData() throws Exception {
        addStorageEngine(false);
        String statement = "select * from ln.wf03";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "select * from ln";
        expect =
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
    }

    // @Test
    public void testWriteAndQueryAfterCEOriHasDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "| key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "|  77|               null|                    null|          null|           null|               true|                    null|\n"
                        + "| 100|               true|                    null|          true|             v1|               null|                    null|\n"
                        + "| 200|              false|                   20.71|          null|           null|              false|                   77.71|\n"
                        + "| 400|               null|                    null|         false|             v4|               null|                    null|\n"
                        + "| 800|               null|                    null|          null|             v8|               null|                    null|\n"
                        + "|1600|               null|                    null|          null|            v48|               null|                    null|\n"
                        + "+----+-------------------+------------------------+--------------+---------------+-------------------+------------------------+\n"
                        + "Total line number = 6\n";
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

    public void fusionTestOriHasDataExpHasData() throws Exception {
        session.executeSql("INSERT INTO new.ln (key,status) values(233,3399);");
        String statement = "select * from *";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+-------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf03.wt01.status|ln.wf03.wt01.temperature|new.ln.status|\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+-------------+\n"
                        + "| 77|               null|                    null|               true|                    null|         null|\n"
                        + "|100|               true|                    null|               null|                    null|         null|\n"
                        + "|200|              false|                   20.71|              false|                   77.71|         null|\n"
                        + "|233|               null|                    null|               null|                    null|         3399|\n"
                        + "+---+-------------------+------------------------+-------------------+------------------------+-------------+\n"
                        + "Total line number = 4";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "show time series";
        expect =
                "ResultSets:\n"
                        + "+------------------------+-------+\n"
                        + "|                    path|   type|\n"
                        + "+------------------------+-------+\n"
                        + "|     ln.wf01.wt01.status|BOOLEAN|\n"
                        + "|ln.wf01.wt01.temperature|  FLOAT|\n"
                        + "|     ln.wf03.wt01.status|BOOLEAN|\n"
                        + "|ln.wf03.wt01.temperature| DOUBLE|\n"
                        + "|           new.ln.status|   LONG|\n"
                        + "+------------------------+-------+\n"
                        + "Total line number = 5";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCEOriNoDataExpHasData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+----+--------------+---------------+-------------------+------------------------+\n"
                        + "| key|ln.wf02.status|ln.wf02.version|ln.wf03.wt01.status|ln.wf03.wt01.temperature|\n"
                        + "+----+--------------+---------------+-------------------+------------------------+\n"
                        + "|  77|          null|           null|               true|                    null|\n"
                        + "| 100|          true|             v1|               null|                    null|\n"
                        + "| 200|          null|           null|              false|                   77.71|\n"
                        + "| 400|         false|             v4|               null|                    null|\n"
                        + "| 800|          null|             v8|               null|                    null|\n"
                        + "|1600|          null|            v48|               null|                    null|\n"
                        + "+----+--------------+---------------+-------------------+------------------------+\n"
                        + "Total line number = 6\n";
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

    public void fusionTestOriNoDataExpHasData() throws Exception {
        session.executeSql("INSERT INTO new.ln (key,status) values(233,3399);");
        String statement = "select * from *";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "|key|ln.wf03.wt01.status|ln.wf03.wt01.temperature|new.ln.status|\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "| 77|               true|                    null|         null|\n"
                        + "|200|              false|                   77.71|         null|\n"
                        + "|233|               null|                    null|         3399|\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "Total line number = 3";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "show time series";
        expect =
                "ResultSets:\n"
                        + "+------------------------+-------+\n"
                        + "|                    path|   type|\n"
                        + "+------------------------+-------+\n"
                        + "|     ln.wf03.wt01.status|BOOLEAN|\n"
                        + "|ln.wf03.wt01.temperature| DOUBLE|\n"
                        + "|           new.ln.status|   LONG|\n"
                        + "+------------------------+-------+\n"
                        + "Total line number = 3";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCEOriHasDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");

        String statement = "select * from ln";
        String expect =
                "ResultSets:\n"
                        + "+----+-------------------+------------------------+--------------+---------------+\n"
                        + "| key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|ln.wf02.status|ln.wf02.version|\n"
                        + "+----+-------------------+------------------------+--------------+---------------+\n"
                        + "| 100|               true|                    null|          true|             v1|\n"
                        + "| 200|              false|                   20.71|          null|           null|\n"
                        + "| 400|               null|                    null|         false|             v4|\n"
                        + "| 800|               null|                    null|          null|             v8|\n"
                        + "|1600|               null|                    null|          null|            v48|\n"
                        + "+----+-------------------+------------------------+--------------+---------------+\n"
                        + "Total line number = 5\n";
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

    public void fusionTestOriHasDataExpNoData() throws Exception {
        session.executeSql("INSERT INTO new.ln (key,status) values(233,3399);");
        String statement = "select * from *";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "|key|ln.wf01.wt01.status|ln.wf01.wt01.temperature|new.ln.status|\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "|100|               true|                    null|         null|\n"
                        + "|200|              false|                   20.71|         null|\n"
                        + "|233|               null|                    null|         3399|\n"
                        + "+---+-------------------+------------------------+-------------+\n"
                        + "Total line number = 3";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "show time series";
        expect =
                "ResultSets:\n"
                        + "+------------------------+-------+\n"
                        + "|                    path|   type|\n"
                        + "+------------------------+-------+\n"
                        + "|     ln.wf01.wt01.status|BOOLEAN|\n"
                        + "|ln.wf01.wt01.temperature|  FLOAT|\n"
                        + "|           new.ln.status|   LONG|\n"
                        + "+------------------------+-------+\n"
                        + "Total line number = 3";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    public void testWriteAndQueryAfterCEOriNoDataExpNoData() throws Exception {
        session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");

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

    public void fusionTestOriNoDataExpNoData() throws Exception {
        session.executeSql("INSERT INTO new.ln (key,status) values(233,3399);");
        String statement = "select * from *";
        String expect =
                "ResultSets:\n"
                        + "+---+-------------+\n"
                        + "|key|new.ln.status|\n"
                        + "+---+-------------+\n"
                        + "|233|         3399|\n"
                        + "+---+-------------+\n"
                        + "Total line number = 1";
        SQLTestTools.executeAndCompare(session, statement, expect);

        statement = "show time series";
        expect =
                "ResultSets:\n"
                        + "+-------------+----+\n"
                        + "|         path|type|\n"
                        + "+-------------+----+\n"
                        + "|new.ln.status|LONG|\n"
                        + "+-------------+----+\n"
                        + "Total line number = 1";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }
}
