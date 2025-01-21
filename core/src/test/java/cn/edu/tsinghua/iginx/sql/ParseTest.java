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
package cn.edu.tsinghua.iginx.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cn.edu.tsinghua.iginx.engine.shared.expr.BaseExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.FuncExpression;
import cn.edu.tsinghua.iginx.engine.shared.expr.KeyExpression;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.sql.statement.AddStorageEngineStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteColumnsStatement;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertFromSelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.ShowReplicationStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.FromPartType;
import cn.edu.tsinghua.iginx.sql.statement.frompart.SubQueryFromPart;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinCondition;
import cn.edu.tsinghua.iginx.sql.statement.frompart.join.JoinType;
import cn.edu.tsinghua.iginx.sql.statement.select.SelectStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import cn.edu.tsinghua.iginx.thrift.StorageEngine;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    assertEquals(2, statement.getKeys().size());
  }

  @Test
  public void testParseInsertWithSubQuery() {
    String insertStr =
        "INSERT INTO test.copy (key, status, hardware, num) values (SELECT status, hardware, num FROM test) TIME_OFFSET = 5;";
    InsertFromSelectStatement statement =
        (InsertFromSelectStatement) TestUtils.buildStatement(insertStr);

    InsertStatement insertStatement = statement.getSubInsertStatement();
    assertEquals("test.copy", insertStatement.getPrefixPath());

    List<String> paths = Arrays.asList("test.copy.status", "test.copy.hardware", "test.copy.num");
    assertEquals(paths, insertStatement.getPaths());

    SelectStatement selectStatement = statement.getSubSelectStatement();

    HashSet<String> pathSet =
        new HashSet<>(Arrays.asList("test.status", "test.hardware", "test.num"));
    assertEquals(pathSet, selectStatement.getPathSet());

    assertEquals(5, statement.getKeyOffset());
  }

  @Test
  public void testParseSelect() {
    String selectStr =
        "SELECT SUM(c), SUM(d), SUM(e), COUNT(f), COUNT(g) FROM a.b WHERE 100 < key and key < 1000 or d == \"abc\" or \"666\" <= c or (e < 10 and not (f < 10)) OVER WINDOW (size 10 IN [200, 300));";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(selectStr);

    assertTrue(statement.hasValueFilter());
    assertTrue(statement.hasDownsample());
    assertEquals(UnarySelectStatement.QueryType.DownSampleQuery, statement.getQueryType());

    List<FuncExpression> funcExprList = statement.getTargetTypeFuncExprList(MappingType.SetMapping);
    assertEquals(5, funcExprList.size());
    assertEquals("sum(a.b.c)", funcExprList.get(0).getColumnName());
    assertEquals("sum(a.b.d)", funcExprList.get(1).getColumnName());
    assertEquals("sum(a.b.e)", funcExprList.get(2).getColumnName());
    assertEquals("count(a.b.f)", funcExprList.get(3).getColumnName());
    assertEquals("count(a.b.g)", funcExprList.get(4).getColumnName());

    assertEquals(
        "(((key > 100 && key < 1000) || a.b.d == \"abc\" || a.b.c >= \"666\" || (a.b.e < 10 && !(a.b.f < 10))) && key >= 200 && key < 300)",
        statement.getFilter().toString());

    assertEquals(200, statement.getStartKey());
    assertEquals(300, statement.getEndKey());
    assertEquals(10, statement.getPrecision());
  }

  @Test
  public void testFilter() {
    String selectStr = "SELECT a FROM root WHERE a > 100;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(selectStr);
    assertEquals(new HashSet<>(Collections.singletonList("root.a")), statement.getPathSet());
    assertEquals("root.a > 100", statement.getFilter().toString());

    selectStr = "SELECT a, b FROM root WHERE a > b;";
    statement = (UnarySelectStatement) TestUtils.buildStatement(selectStr);
    assertEquals(new HashSet<>(Arrays.asList("root.a", "root.b")), statement.getPathSet());
    assertEquals("root.a > root.b", statement.getFilter().toString());
  }

  @Test
  public void testParseGroupBy() {
    String selectStr = "SELECT MAX(c) FROM a.b OVER WINDOW (size 10 IN [100, 1000));";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(selectStr);
    assertEquals(100, statement.getStartKey());
    assertEquals(1000, statement.getEndKey());
    assertEquals(10L, statement.getPrecision());
  }

  @Test
  public void testParseSpecialClause() {
    String limit = "SELECT a FROM test LIMIT 2, 5;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(limit);
    assertEquals(5, statement.getLimit());
    assertEquals(2, statement.getOffset());

    String orderBy = "SELECT a FROM test ORDER BY KEY";
    statement = (UnarySelectStatement) TestUtils.buildStatement(orderBy);
    assertEquals(1, statement.getOrderByExpressions().size());
    assertTrue(
        statement
            .getOrderByExpressions()
            .get(0)
            .equalExceptAlias(new KeyExpression(SQLConstant.KEY)));
    assertTrue(statement.getAscendingList().get(0));

    String orderByAndLimit = "SELECT a FROM test ORDER BY a DESC LIMIT 10 OFFSET 5;";
    statement = (UnarySelectStatement) TestUtils.buildStatement(orderByAndLimit);
    assertEquals(1, statement.getOrderByExpressions().size());
    assertTrue(
        statement.getOrderByExpressions().get(0).equalExceptAlias(new BaseExpression("test.a")));
    assertFalse(statement.getAscendingList().get(0));
    assertEquals(5, statement.getOffset());
    assertEquals(10, statement.getLimit());

    String groupBy = "SELECT max(a) FROM test OVER WINDOW (size 5 IN (10, 120])";
    statement = (UnarySelectStatement) TestUtils.buildStatement(groupBy);

    assertEquals(11, statement.getStartKey());
    assertEquals(121, statement.getEndKey());
    assertEquals(5L, statement.getPrecision());

    String groupByAndLimit =
        "SELECT max(a) FROM test OVER WINDOW (size 10 IN (10, 120)) LIMIT 5 OFFSET 2;";
    statement = (UnarySelectStatement) TestUtils.buildStatement(groupByAndLimit);
    assertEquals(11, statement.getStartKey());
    assertEquals(120, statement.getEndKey());
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
  public void testParseDeleteColumns() {
    String deleteColumnsStr = "DELETE COLUMNS a.b.c, a.b.d;";
    DeleteColumnsStatement statement =
        (DeleteColumnsStatement) TestUtils.buildStatement(deleteColumnsStr);
    List<String> paths = Arrays.asList("a.b.c", "a.b.d");
    assertEquals(paths, statement.getPaths());
  }

  @Test
  public void testParseLimitClause() {
    String selectWithLimit = "SELECT * FROM a.b LIMIT 10";
    String selectWithLimitAndOffset01 = "SELECT * FROM a.b LIMIT 2, 10";
    String selectWithLimitAndOffset02 = "SELECT * FROM a.b LIMIT 10 OFFSET 2";
    String selectWithLimitAndOffset03 = "SELECT * FROM a.b OFFSET 2 LIMIT 10";

    UnarySelectStatement statement =
        (UnarySelectStatement) TestUtils.buildStatement(selectWithLimit);
    assertEquals(10, statement.getLimit());
    assertEquals(0, statement.getOffset());

    statement = (UnarySelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset01);
    assertEquals(10, statement.getLimit());
    assertEquals(2, statement.getOffset());

    statement = (UnarySelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset02);
    assertEquals(10, statement.getLimit());
    assertEquals(2, statement.getOffset());

    statement = (UnarySelectStatement) TestUtils.buildStatement(selectWithLimitAndOffset03);
    assertEquals(10, statement.getLimit());
    assertEquals(2, statement.getOffset());
  }

  @Test
  public void testSubQueryClause() {
    String selectWithSubQuery = "SELECT res.max_a FROM (SELECT max(a) AS max_a FROM root AS res);";
    UnarySelectStatement statement =
        (UnarySelectStatement) TestUtils.buildStatement(selectWithSubQuery);
    assertEquals(new HashSet<>(Collections.singletonList("res.max_a")), statement.getPathSet());

    assertEquals(FromPartType.SubQuery, statement.getFromParts().get(0).getType());
    SubQueryFromPart subQueryFromPart = (SubQueryFromPart) statement.getFromParts().get(0);
    UnarySelectStatement subStatement = (UnarySelectStatement) subQueryFromPart.getSubQuery();

    FuncExpression expression =
        subStatement.getTargetTypeFuncExprList(MappingType.SetMapping).get(0);
    assertEquals("max(res.a)", expression.getColumnName());
    assertEquals("max", expression.getFuncName());
    assertEquals("max_a", expression.getAlias());
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
        "ADD STORAGEENGINE (\"127.0.0.1\", 6667, \"iotdb12\", \"username=root, password=root\"), ('127.0.0.1', 6668, 'influxdb', 'key1=val1, key2=val2');";
    AddStorageEngineStatement statement =
        (AddStorageEngineStatement) TestUtils.buildStatement(addStorageEngineStr);

    assertEquals(2, statement.getEngines().size());

    Map<String, String> extra01 = new HashMap<>();
    extra01.put("username", "root");
    extra01.put("password", "root");
    StorageEngine engine01 =
        new StorageEngine("127.0.0.1", 6667, StorageEngineType.iotdb12, extra01);

    Map<String, String> extra02 = new HashMap<>();
    extra02.put("key1", "val1");
    extra02.put("key2", "val2");
    StorageEngine engine02 =
        new StorageEngine("127.0.0.1", 6668, StorageEngineType.influxdb, extra02);

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
    assertEquals(expectedTimes, insertStatement.getKeys());

    String queryStr =
        "SELECT AVG(c) FROM a.b WHERE c > 10 AND c < 1ms OVER WINDOW (size 10 IN [1s, 2s));";
    UnarySelectStatement selectStatement =
        (UnarySelectStatement) TestUtils.buildStatement(queryStr);

    assertEquals(
        "((a.b.c > 10 && a.b.c < 1000000) && key >= 1000000000 && key < 2000000000)",
        selectStatement.getFilter().toString());

    assertEquals(1000000000L, selectStatement.getStartKey());
    assertEquals(2000000000L, selectStatement.getEndKey());
    assertEquals(10L, selectStatement.getPrecision());
  }

  @Test
  public void testJoin() {
    String joinStr = "SELECT * FROM cpu1, cpu2";
    UnarySelectStatement selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    JoinCondition joinCondition =
        new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinStr = "SELECT * FROM cpu1, cpu2, cpu3";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(3, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());
    assertEquals("cpu3", selectStatement.getFromParts().get(2).getPrefix());

    joinCondition = new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinCondition = new JoinCondition(JoinType.CrossJoin, null, Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(2).getJoinCondition());

    joinStr = "SELECT * FROM cpu1 LEFT JOIN cpu2 ON cpu1.usage = cpu2.usage";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    joinCondition =
        new JoinCondition(
            JoinType.LeftOuterJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
            Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinStr = "SELECT * FROM cpu1 RIGHT OUTER JOIN cpu2 USING usage";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    joinCondition =
        new JoinCondition(JoinType.RightOuterJoin, null, Collections.singletonList("usage"));
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinStr = "SELECT * FROM cpu1 FULL OUTER JOIN cpu2 ON cpu1.usage = cpu2.usage";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    joinCondition =
        new JoinCondition(
            JoinType.FullOuterJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
            Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinStr = "SELECT * FROM cpu1 JOIN cpu2 ON cpu1.usage = cpu2.usage";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    joinCondition =
        new JoinCondition(
            JoinType.InnerJoin,
            new PathFilter("cpu1.usage", Op.E, "cpu2.usage"),
            Collections.emptyList());
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());

    joinStr = "SELECT * FROM cpu1 INNER JOIN cpu2 USING usage";
    selectStatement = (UnarySelectStatement) TestUtils.buildStatement(joinStr);

    assertEquals(2, selectStatement.getFromParts().size());
    assertEquals("cpu1", selectStatement.getFromParts().get(0).getPrefix());
    assertEquals("cpu2", selectStatement.getFromParts().get(1).getPrefix());

    joinCondition = new JoinCondition(JoinType.InnerJoin, null, Collections.singletonList("usage"));
    assertEquals(joinCondition, selectStatement.getFromParts().get(1).getJoinCondition());
  }
}
