package cn.edu.tsinghua.iginx.sql;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.sql.expression.BaseExpression;
import cn.edu.tsinghua.iginx.sql.statement.*;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import java.util.*;
import org.junit.Test;

public class ParseTest {

    @Test
    public void testParseInsert() {
        String insertStr =
                "INSERT INTO a.b.c (key, status, hardware, num) values (1, NaN, Null, 1627399423055), (2, false, \"v2\", 1627399423056);";
        InsertStatement statement = (InsertStatement) TestUtils.buildStatement(insertStr);
        assertEquals("a.b.c", statement.getPrefixPath());

        List<String> paths = Arrays.asList("a.b.c.hardware", "a.b.c.num", "a.b.c.status");
        assertEquals(paths, statement.getPaths());

        assertEquals(2, statement.getTimes().size());

        insertStr =
                "SELECT avg_s1 FROM (SELECT AVG(s1) AS avg_s1 FROM us.d1 OVER (RANGE 100 IN [1000, 1600)) WHERE avg_s1 > 1200;";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(insertStr);
        System.out.println();
    }

    @Test
    public void testParseInsertWithSubQuery() {
        String insertStr =
                "INSERT INTO test.copy (key, status, hardware, num) values (SELECT status, hardware, num FROM test) TIME_OFFSET = 5;";
        InsertFromSelectStatement statement =
                (InsertFromSelectStatement) TestUtils.buildStatement(insertStr);

        InsertStatement insertStatement = statement.getSubInsertStatement();
        assertEquals("test.copy", insertStatement.getPrefixPath());

        List<String> paths =
                Arrays.asList("test.copy.status", "test.copy.hardware", "test.copy.num");
        assertEquals(paths, insertStatement.getPaths());

        SelectStatement selectStatement = statement.getSubSelectStatement();

        paths = Arrays.asList("test.status", "test.hardware", "test.num");
        assertEquals(paths, selectStatement.getSelectedPaths());

        assertEquals(5, statement.getTimeOffset());
    }

    @Test
    public void testParseSelect() {
        String selectStr =
                "SELECT SUM(c), SUM(d), SUM(e), COUNT(f), COUNT(g) FROM a.b WHERE 100 < key and key < 1000 or d == \"abc\" or \"666\" <= c or (e < 10 and not (f < 10)) OVER (RANGE 10 IN [200, 300)) AGG LEVEL = 2, 3;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);

        assertTrue(statement.hasFunc());
        assertTrue(statement.hasValueFilter());
        assertTrue(statement.hasDownsample());
        assertEquals(SelectStatement.QueryType.DownSampleQuery, statement.getQueryType());

        assertEquals(2, statement.getBaseExpressionMap().size());
        assertTrue(statement.getBaseExpressionMap().containsKey("sum"));
        assertTrue(statement.getBaseExpressionMap().containsKey("count"));

        assertEquals("a.b.c", statement.getBaseExpressionMap().get("sum").get(0).getPathName());
        assertEquals("a.b.d", statement.getBaseExpressionMap().get("sum").get(1).getPathName());
        assertEquals("a.b.e", statement.getBaseExpressionMap().get("sum").get(2).getPathName());
        assertEquals("a.b.f", statement.getBaseExpressionMap().get("count").get(0).getPathName());
        assertEquals("a.b.g", statement.getBaseExpressionMap().get("count").get(1).getPathName());

        assertEquals(
                "(((key > 100 && key < 1000) || a.b.d == \"abc\" || a.b.c >= \"666\" || (a.b.e < 10 && !(a.b.f < 10))) && key >= 200 && key < 300)",
                statement.getFilter().toString());

        assertEquals(200, statement.getStartTime());
        assertEquals(300, statement.getEndTime());
        assertEquals(10, statement.getPrecision());

        assertEquals(Arrays.asList(2, 3), statement.getLayers());
    }

    @Test
    public void testFilter() {
        String selectStr = "SELECT a FROM root WHERE a > 100;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Collections.singletonList("root.a")), statement.getPathSet());
        assertEquals("root.a > 100", statement.getFilter().toString());

        selectStr = "SELECT a, b FROM root WHERE a > b;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(new HashSet<>(Arrays.asList("root.a", "root.b")), statement.getPathSet());
        assertEquals("root.a > root.b", statement.getFilter().toString());
    }

    @Test
    public void testParseGroupBy() {
        String selectStr = "SELECT MAX(c) FROM a.b OVER (RANGE 10 IN [100, 1000));";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals(100, statement.getStartTime());
        assertEquals(1000, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());

        selectStr = "SELECT SUM(c) FROM a.b AGG LEVEL = 1, 2;";
        statement = (SelectStatement) TestUtils.buildStatement(selectStr);
        assertEquals("a.b.c", statement.getBaseExpressionMap().get("sum").get(0).getPathName());
        assertEquals(Arrays.asList(1, 2), statement.getLayers());
    }

    @Test
    public void testParseSpecialClause() {
        String limit = "SELECT a FROM test LIMIT 2, 5;";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(limit);
        assertEquals(5, statement.getLimit());
        assertEquals(2, statement.getOffset());

        String orderBy = "SELECT a FROM test ORDER BY KEY";
        statement = (SelectStatement) TestUtils.buildStatement(orderBy);
        assertEquals(Collections.singletonList(SQLConstant.KEY), statement.getOrderByPaths());
        assertTrue(statement.isAscending());

        String orderByAndLimit = "SELECT a FROM test ORDER BY a DESC LIMIT 10 OFFSET 5;";
        statement = (SelectStatement) TestUtils.buildStatement(orderByAndLimit);
        assertEquals(Collections.singletonList("test.a"), statement.getOrderByPaths());
        assertFalse(statement.isAscending());
        assertEquals(5, statement.getOffset());
        assertEquals(10, statement.getLimit());

        String groupBy = "SELECT max(a) FROM test OVER (RANGE 5 IN (10, 120])";
        statement = (SelectStatement) TestUtils.buildStatement(groupBy);

        assertEquals(11, statement.getStartTime());
        assertEquals(121, statement.getEndTime());
        assertEquals(5L, statement.getPrecision());

        String groupByAndLimit =
                "SELECT max(a) FROM test OVER (RANGE 10 IN (10, 120)) LIMIT 5 OFFSET 2;";
        statement = (SelectStatement) TestUtils.buildStatement(groupByAndLimit);
        assertEquals(11, statement.getStartTime());
        assertEquals(120, statement.getEndTime());
        assertEquals(10L, statement.getPrecision());
        assertEquals(2, statement.getOffset());
        assertEquals(5, statement.getLimit());
    }

    @Test
    public void testParseDelete() {
        String deleteStr =
                "DELETE FROM a.b.c, a.b.d WHERE key > 1627464728862 AND key < 2022-12-12 16:18:23+1s;";
        DeleteStatement statement = (DeleteStatement) TestUtils.buildStatement(deleteStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());
    }

    @Test
    public void testParseDeleteTimeSeries() {
        String deleteTimeSeriesStr = "DELETE TIME SERIES a.b.c, a.b.d;";
        DeleteTimeSeriesStatement statement =
                (DeleteTimeSeriesStatement) TestUtils.buildStatement(deleteTimeSeriesStr);
        List<String> paths = Arrays.asList("a.b.c", "a.b.d");
        assertEquals(paths, statement.getPaths());
    }

    @Test
    public void testParseLimitClause() {
        String selectWithLimit = "SELECT * FROM a.b LIMIT 10";
        String selectWithLimitAndOffset01 = "SELECT * FROM a.b LIMIT 2, 10";
        String selectWithLimitAndOffset02 = "SELECT * FROM a.b LIMIT 10 OFFSET 2";
        String selectWithLimitAndOffset03 = "SELECT * FROM a.b OFFSET 2 LIMIT 10";

        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectWithLimit);
        assertEquals(10, statement.getLimit());
        assertEquals(0, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset01);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset02);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());

        statement = (SelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset03);
        assertEquals(10, statement.getLimit());
        assertEquals(2, statement.getOffset());
    }

    @Test
    public void testSubQueryClause() {
        String selectWithSubQuery =
                "SELECT res.max_a FROM (SELECT max(a) AS max_a FROM root AS res);";
        SelectStatement statement = (SelectStatement) TestUtils.buildStatement(selectWithSubQuery);
        assertEquals(Collections.singletonList("res.max_a"), statement.getSelectedPaths());

        assertEquals(FromPartType.SubQueryFromPart, statement.getFromParts().get(0).getType());
        SubQueryFromPart subQueryFromPart = (SubQueryFromPart) statement.getFromParts().get(0);
        SelectStatement subStatement = subQueryFromPart.getSubQuery();

        BaseExpression expression = subStatement.getBaseExpressionMap().get("max").get(0);
        assertEquals("root.a", expression.getPathName());
        assertEquals("max", expression.getFuncName());
        assertEquals("res.max_a", expression.getAlias());
    }

    @Test
    public void testParseShowReplication() {
        String showReplicationStr = "SHOW REPLICA NUMBER";
        ShowReplicationStatement statement =
                (ShowReplicationStatement) TestUtils.buildStatement(showReplicationStr);
        assertEquals(StatementType.SHOW_REPLICATION, statement.statementType);
    }

    @Test
    public void testParseAddStorageEngine() {
        String addStorageEngineStr =
                "ADD STORAGEENGINE (\"127.0.0.1\", 6667, \"iotdb12\", \"username: root, password: root\"), (\"127.0.0.1\", 6668, \"influxdb\", \"key1: val1, key2: val2\");";
        AddStorageEngineStatement statement =
                (AddStorageEngineStatement) TestUtils.buildStatement(addStorageEngineStr);

        assertEquals(2, statement.getEngines().size());

        Map<String, String> extra01 = new HashMap<>();
        extra01.put("username", "root");
        extra01.put("password", "root");
        StorageEngine engine01 = new StorageEngine("127.0.0.1", 6667, "iotdb12", extra01);

        Map<String, String> extra02 = new HashMap<>();
        extra02.put("key1", "val1");
        extra02.put("key2", "val2");
        StorageEngine engine02 = new StorageEngine("127.0.0.1", 6668, "influxdb", extra02);

        assertEquals(engine01, statement.getEngines().get(0));
        assertEquals(engine02, statement.getEngines().get(1));
    }

    @Test
    public void testParseTimeWithUnit() {
        String insertStr =
                "INSERT INTO a.b (key, c) values "
                        + "(1, 1), "
                        + "(2ns, 2), "
                        + "(3us, 3), "
                        + "(4ms, 4), "
                        + "(5s, 5);";
        InsertStatement insertStatement = (InsertStatement) TestUtils.buildStatement(insertStr);

        List<Long> expectedTimes = Arrays.asList(1L, 2L, 3000L, 4000000L, 5000000000L);
        assertEquals(expectedTimes, insertStatement.getTimes());

        String queryStr =
                "SELECT AVG(c) FROM a.b WHERE c > 10 AND c < 1ms OVER (RANGE 10 IN [1s, 2s));";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(queryStr);

        assertEquals(
                "((a.b.c > 10 && a.b.c < 1000000) && key >= 1000000000 && key < 2000000000)",
                selectStatement.getFilter().toString());

        assertEquals(1000000000L, selectStatement.getStartTime());
        assertEquals(2000000000L, selectStatement.getEndTime());
        assertEquals(10L, selectStatement.getPrecision());
    }

    @Test
    public void testJoin() {
        String joinStr = "SELECT * FROM cpu1, cpu2";
        SelectStatement selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        JoinCondition joinCondition =
                new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinStr = "SELECT * FROM cpu1, cpu2, cpu3";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(3, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());
        assertEquals("cpu3", selectStatement.getFromParts().get(2).getPath());

        joinCondition = new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinCondition = new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(2).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(2).getJoinCondition());

        joinStr = "SELECT * FROM cpu1 LEFT JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        joinCondition =
                new JoinCondition(
                        JoinType.LeftOuterJoin,
                        new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
                        Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinStr = "SELECT * FROM cpu1 RIGHT OUTER JOIN cpu2 USING usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        joinCondition =
                new JoinCondition(
                        JoinType.RightOuterJoin, null, Collections.singletonList("usage"));
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinStr = "SELECT * FROM cpu1 FULL OUTER JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        joinCondition =
                new JoinCondition(
                        JoinType.FullOuterJoin,
                        new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
                        Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinStr = "SELECT * FROM cpu1 JOIN cpu2 ON cpu1.usage = cpu2.usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        joinCondition =
                new JoinCondition(
                        JoinType.InnerJoin,
                        new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
                        Collections.emptyList());
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

        joinStr = "SELECT * FROM cpu1 INNER JOIN cpu2 USING usage";
        selectStatement = (SelectStatement) TestUtils.buildStatement(joinStr);

        assertEquals(2, selectStatement.getFromParts().size());
        assertEquals("cpu1", selectStatement.getFromParts().get(0).getPath());
        assertEquals("cpu2", selectStatement.getFromParts().get(1).getPath());

        joinCondition =
                new JoinCondition(JoinType.InnerJoin, null, Collections.singletonList("usage"));
        assertTrue(selectStatement.getFromParts().get(1).isJoinPart());
        assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());
    }
}
