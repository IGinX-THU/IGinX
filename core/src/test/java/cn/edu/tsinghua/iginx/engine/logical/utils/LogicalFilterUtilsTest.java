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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.logical.utils;

import static org.junit.Assert.assertEquals;

import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.NotFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.OrFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.metadata.entity.ColumnsInterval;
import cn.edu.tsinghua.iginx.sql.TestUtils;
import cn.edu.tsinghua.iginx.sql.exception.SQLParserException;
import cn.edu.tsinghua.iginx.sql.statement.DeleteStatement;
import cn.edu.tsinghua.iginx.sql.statement.select.UnarySelectStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import org.junit.Test;

class InfluxDBFilterTransformer {

  public static String toString(Filter filter) {
    switch (filter.getType()) {
      case And:
        return toString((AndFilter) filter);
      case Or:
        return toString((OrFilter) filter);
      case Not:
        return toString((NotFilter) filter);
      case Value:
        return toString((ValueFilter) filter);
      case Key:
        return toString((KeyFilter) filter);
      default:
        return "";
    }
  }

  private static String toString(AndFilter filter) {
    return filter.getChildren().stream()
        .map(InfluxDBFilterTransformer::toString)
        .collect(Collectors.joining(" and ", "(", ")"));
  }

  private static String toString(NotFilter filter) {
    return "not " + filter.toString();
  }

  private static String toString(KeyFilter filter) {
    return "time " + Op.op2Str(filter.getOp()) + " " + filter.getValue();
  }

  private static String toString(ValueFilter filter) {
    return filter.getPath() + " " + Op.op2Str(filter.getOp()) + " " + filter.getValue().getValue();
  }

  private static String toString(OrFilter filter) {
    return filter.getChildren().stream()
        .map(InfluxDBFilterTransformer::toString)
        .collect(Collectors.joining(" or ", "(", ")"));
  }
}

public class LogicalFilterUtilsTest {
  @Test
  public void testRemoveNot() {
    String select = "SELECT a FROM root WHERE !(a != 10);";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    Filter filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.removeNot(filter).toString());

    select = "SELECT a FROM root WHERE !(!(a != 10));";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.removeNot(filter).toString());

    select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 AND c > 7 AND d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.removeNot(filter).toString());

    select = "SELECT a FROM root WHERE !(a > 5 AND b <= 10 or c > 7 AND d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.removeNot(filter).toString());
  }

  @Test
  public void testToDNF() {
    String select = "SELECT a FROM root WHERE a > 5 AND b <= 10 OR c > 7 AND d == 8;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    Filter filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.toDNF(filter).toString());

    select = "SELECT a FROM root WHERE (a > 5 OR b <= 10) AND (c > 7 OR d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.toDNF(filter).toString());

    select =
        "SELECT a FROM root WHERE (a > 5 OR b <= 10) AND (c > 7 OR d == 8) AND (e < 3 OR f != 2);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.toDNF(filter).toString());

    select = "SELECT a FROM root WHERE (a > 5 AND b <= 10) AND (c > 7 OR d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(LogicalFilterUtils.toDNF(filter).toString());
  }

  @Test
  public void testToCNF() {
    String select = "SELECT a FROM root WHERE a > 5 OR b <= 10 AND c > 7 OR d == 8;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    Filter filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(InfluxDBFilterTransformer.toString(filter));
    System.out.println(LogicalFilterUtils.toCNF(filter).toString());
    System.out.println(InfluxDBFilterTransformer.toString(LogicalFilterUtils.toCNF(filter)));

    select = "SELECT a FROM root WHERE (a > 5 AND b <= 10) OR (c > 7 AND d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(InfluxDBFilterTransformer.toString(filter));
    System.out.println(LogicalFilterUtils.toCNF(filter).toString());
    System.out.println(InfluxDBFilterTransformer.toString(LogicalFilterUtils.toCNF(filter)));

    select =
        "SELECT a FROM root WHERE (a > 5 AND b <= 10) OR (c > 7 OR d == 8) OR (e < 3 AND f != 2);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(InfluxDBFilterTransformer.toString(filter));
    System.out.println(LogicalFilterUtils.toCNF(filter).toString());
    System.out.println(InfluxDBFilterTransformer.toString(LogicalFilterUtils.toCNF(filter)));

    select = "SELECT a FROM root WHERE (a > 5 OR b <= 10) OR (c > 7 AND d == 8);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    System.out.println(filter.toString());
    System.out.println(InfluxDBFilterTransformer.toString(filter));
    System.out.println(LogicalFilterUtils.toCNF(filter).toString());
    System.out.println(InfluxDBFilterTransformer.toString(LogicalFilterUtils.toCNF(filter)));
  }

  @Test
  public void testTimeRange() {
    String delete = "DELETE FROM root.a WHERE (key > 5 AND key <= 10) OR (key > 12 AND key < 15);";
    DeleteStatement statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(
        Arrays.asList(new KeyRange(6, 11), new KeyRange(13, 15)), statement.getKeyRanges());

    delete =
        "DELETE FROM root.a WHERE (key > 1 AND key <= 8) OR (key >= 5 AND key < 11) OR key >= 66;";
    statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(
        Arrays.asList(new KeyRange(2, 11), new KeyRange(66, Long.MAX_VALUE)),
        statement.getKeyRanges());

    delete = "DELETE FROM root.a WHERE key >= 16 AND key < 61;";
    statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(Collections.singletonList(new KeyRange(16, 61)), statement.getKeyRanges());

    delete = "DELETE FROM root.a WHERE key >= 16;";
    statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(
        Collections.singletonList(new KeyRange(16, Long.MAX_VALUE)), statement.getKeyRanges());

    delete = "DELETE FROM root.a WHERE key < 61;";
    statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(Collections.singletonList(new KeyRange(0, 61)), statement.getKeyRanges());

    delete = "DELETE FROM root.a;";
    statement = (DeleteStatement) TestUtils.buildStatement(delete);
    assertEquals(
        Collections.singletonList(new KeyRange(0, Long.MAX_VALUE)), statement.getKeyRanges());
  }

  @Test(expected = SQLParserException.class)
  public void testErrDelete() {
    String delete = "DELETE FROM root.a WHERE key < 61 AND key > 616;";
    TestUtils.buildStatement(delete);
  }

  @Test
  public void testGetSubFilterFromFragment() {
    // sub1
    String select =
        "SELECT a FROM root WHERE (a > 5 OR d < 15) AND !(e < 27) AND (c < 10 OR b > 2) AND key > 10 AND key <= 100;";
    UnarySelectStatement statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    Filter filter = statement.getFilter();
    assertEquals(
        "((root.a > 5 || root.d < 15) && !(root.e < 27) && (root.c < 10 || root.b > 2) && key > 10 && key <= 100)",
        filter.toString());
    assertEquals(
        "(key > 10 && key <= 100)",
        LogicalFilterUtils.getSubFilterFromFragment(filter, new ColumnsInterval("root.a", "root.c"))
            .toString());

    // sub2
    select =
        "SELECT a FROM root WHERE (a > 5 OR d < 15) AND !(e < 27) AND (c < 10 OR b > 2) AND key > 10 AND key <= 100;";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    assertEquals(
        "((root.a > 5 || root.d < 15) && !(root.e < 27) && (root.c < 10 || root.b > 2) && key > 10 && key <= 100)",
        filter.toString());
    assertEquals(
        "(root.e >= 27 && key > 10 && key <= 100)",
        LogicalFilterUtils.getSubFilterFromFragment(filter, new ColumnsInterval("root.c", "root.z"))
            .toString());

    // whole
    select = "SELECT a FROM root WHERE (a > 5 OR d < 15) AND !(e < 27) AND (c < 10 OR b > 2);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    assertEquals(
        "((root.a > 5 || root.d < 15) && !(root.e < 27) && (root.c < 10 || root.b > 2))",
        filter.toString());
    assertEquals(
        "((root.a > 5 || root.d < 15) && root.e >= 27 && (root.c < 10 || root.b > 2))",
        LogicalFilterUtils.getSubFilterFromFragment(filter, new ColumnsInterval("root.a", "root.z"))
            .toString());

    // empty
    select = "SELECT a FROM root WHERE (a > 5 OR d < 15) AND !(e < 27) AND (c < 10 OR b > 2);";
    statement = (UnarySelectStatement) TestUtils.buildStatement(select);
    filter = statement.getFilter();
    assertEquals(
        "((root.a > 5 || root.d < 15) && !(root.e < 27) && (root.c < 10 || root.b > 2))",
        filter.toString());
    assertEquals(
        "True",
        LogicalFilterUtils.getSubFilterFromFragment(filter, new ColumnsInterval("root.h", "root.z"))
            .toString());
  }
}
