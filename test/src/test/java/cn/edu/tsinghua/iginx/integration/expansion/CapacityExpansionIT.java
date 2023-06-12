package cn.edu.tsinghua.iginx.integration.expansion;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.integration.controller.Controller;
import cn.edu.tsinghua.iginx.integration.expansion.utils.SQLTestTools;
import cn.edu.tsinghua.iginx.integration.tool.DBType;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.RemovedStorageEngineInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 原始节点相关的变量命名统一用 ori 扩容节点相关的变量命名统一用 exp */
public abstract class CapacityExpansionIT implements BaseCapacityExpansionIT {

    private static final Logger logger = LoggerFactory.getLogger(CapacityExpansionIT.class);

    protected static Session session;

    protected DBType dbType;

    public CapacityExpansionIT(DBType dbType) {
        this.dbType = dbType;
    }

    @BeforeClass
    public static void setUp() {
        try {
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
        } catch (SessionException e) {
            logger.error("open session error: {}", e.getMessage());
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            logger.error("close session error: {}", e.getMessage());
        }
    }

    @After
    public void clearData() {
        Controller.clearData(session);
    }

    @Test
    public void oriHasDataExpHasData() {
        // 查询原始节点的历史数据，结果不为空
        testQueryHistoryDataOriHasData();
        // 写入并查询新数据
        testWriteAndQueryNewData();
        // 扩容
        addStorageEngine(true);
        // 查询扩容节点的历史数据，结果不为空
        testQueryHistoryDataExpHasData();
        // 再次查询新数据
        queryNewData();
        // 再次写入并查询所有新数据
        testWriteAndQueryNewDataAfterCE();
    }

    @Test
    public void oriHasDataExpNoData() {
        // 查询原始节点的历史数据，结果不为空
        testQueryHistoryDataOriHasData();
        // 写入并查询新数据
        testWriteAndQueryNewData();
        // 扩容
        addStorageEngine(false);
        // 查询扩容节点的历史数据，结果为空
        testQueryHistoryDataExpNoData();
        // 再次查询新数据
        queryNewData();
        // 再次写入并查询所有新数据
        testWriteAndQueryNewDataAfterCE();
    }

    @Test
    public void oriNoDataExpHasData() {
        // 查询原始节点的历史数据，结果为空
        testQueryHistoryDataOriNoData();
        // 写入并查询新数据
        testWriteAndQueryNewData();
        // 扩容
        addStorageEngine(true);
        // 查询扩容节点的历史数据，结果不为空
        testQueryHistoryDataExpHasData();
        // 再次查询新数据
        queryNewData();
        // 再次写入并查询所有新数据
        testWriteAndQueryNewDataAfterCE();
        // 测试带前缀的添加和移除存储引擎操作
        testAddAndRemoveStorageEngineWithPrefix();
    }

    @Test
    public void oriNoDataExpNoData() {
        // 查询原始节点的历史数据，结果为空
        testQueryHistoryDataOriNoData();
        // 写入并查询新数据
        testWriteAndQueryNewData();
        // 扩容
        addStorageEngine(false);
        // 查询扩容节点的历史数据，结果为空
        testQueryHistoryDataExpNoData();
        // 再次查询新数据
        queryNewData();
        // 再次写入并查询所有新数据
        testWriteAndQueryNewDataAfterCE();
    }

    private void testAddAndRemoveStorageEngineWithPrefix() {
        addStorageEngineWithPrefix("mn", "p1");
        addStorageEngineWithPrefix("mn", "p2");

        String statement = "select * from p1.mn";
        List<String> pathList =
                Arrays.asList("p1.mn.wf03.wt01.status", "p1.mn.wf03.wt01.temperature");

        List<List<Object>> valuesList = BaseHistoryDataGenerator.VALUES_LIST_EXP;
        SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

        statement = "select * from p2.mn";
        pathList = Arrays.asList("p2.mn.wf03.wt01.status", "p2.mn.wf03.wt01.temperature");
        SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

        List<RemovedStorageEngineInfo> removedStorageEngineList = new ArrayList<>();
        removedStorageEngineList.add(
                new RemovedStorageEngineInfo("127.0.0.1", getPort(), "p2", "mn"));
        try {
            session.removeHistoryDataSource(removedStorageEngineList);
        } catch (ExecutionException | SessionException e) {
            logger.error(
                    "remove history data source through session api error: {}", e.getMessage());
        }
        statement = "select * from p2.mn";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);

        try {
            session.executeSql(
                    "remove historydataresource (\"127.0.0.1\", "
                            + getPort()
                            + ", \"p1\", \"mn\")");
        } catch (ExecutionException | SessionException e) {
            logger.error("remove history data source through sql error: {}", e.getMessage());
        }
        statement = "select * from p1.mn";
        expect = "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testQueryHistoryDataOriHasData() {
        String statement = "select * from mn";
        List<String> pathList = BaseHistoryDataGenerator.PATH_LIST_ORI;
        List<List<Object>> valuesList = BaseHistoryDataGenerator.VALUES_LIST_ORI;
        SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);

        statement = "select count(*) from mn.wf01";
        String expect =
                "ResultSets:\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|count(mn.wf01.wt01.status)|count(mn.wf01.wt01.temperature)|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "|                         2|                              2|\n"
                        + "+--------------------------+-------------------------------+\n"
                        + "Total line number = 1\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testQueryHistoryDataOriNoData() {
        String statement = "select * from mn";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testQueryHistoryDataExpNoData() {
        String statement = "select * from mn.wf03";
        String expect =
                "ResultSets:\n" + "+---+\n" + "|key|\n" + "+---+\n" + "+---+\n" + "Empty set.\n";
        SQLTestTools.executeAndCompare(session, statement, expect);
    }

    private void testQueryHistoryDataExpHasData() {
        String statement = "select * from mn.wf03";
        List<String> pathList = BaseHistoryDataGenerator.PATH_LIST_EXP;
        List<List<Object>> valuesList = BaseHistoryDataGenerator.VALUES_LIST_EXP;
        SQLTestTools.executeAndCompare(session, statement, pathList, valuesList);
    }

    private void testWriteAndQueryNewData() {
        try {
            session.executeSql(
                    "insert into ln.wf02 (key, status, version) values (100, true, \"v1\");");
            session.executeSql(
                    "insert into ln.wf02 (key, status, version) values (400, false, \"v4\");");
            session.executeSql("insert into ln.wf02 (key, version) values (800, \"v8\");");
            queryNewData();
        } catch (ExecutionException | SessionException e) {
            logger.error("insert new data error: {}", e.getMessage());
        }
    }

    private void queryNewData() {
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

    private void testWriteAndQueryNewDataAfterCE() {
        try {
            session.executeSql("insert into ln.wf02 (key, version) values (1600, \"v48\");");
            queryAllNewData();
        } catch (ExecutionException | SessionException e) {
            logger.error("insert new data after capacity expansion error: {}", e.getMessage());
        }
    }

    private void queryAllNewData() {
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
}
