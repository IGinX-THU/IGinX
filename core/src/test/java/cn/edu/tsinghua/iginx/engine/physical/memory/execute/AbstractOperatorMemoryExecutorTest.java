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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_END_COL;
import static cn.edu.tsinghua.iginx.engine.shared.Constants.WINDOW_START_COL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.KeyRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionParams;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Avg;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Last;
import cn.edu.tsinghua.iginx.engine.shared.function.system.Max;
import cn.edu.tsinghua.iginx.engine.shared.operator.CrossJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.Except;
import cn.edu.tsinghua.iginx.engine.shared.operator.InnerJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Intersect;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.MappingTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.MarkJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.OuterJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.Reorder;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.SingleJoin;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.AndFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.BoolFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.KeyFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Op;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.ValueFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.EmptySource;
import cn.edu.tsinghua.iginx.thrift.DataType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public abstract class AbstractOperatorMemoryExecutorTest {

  protected abstract OperatorMemoryExecutor getExecutor();

  private Table generateTableForUnaryOperator(boolean hasTimestamp) {
    Header header;
    List<Field> fields =
        Arrays.asList(
            new Field("a.a.b", DataType.INTEGER),
            new Field("a.b.c", DataType.INTEGER),
            new Field("a.a.c", DataType.INTEGER));
    if (hasTimestamp) {
      header = new Header(Field.KEY, fields);
    } else {
      header = new Header(fields);
    }
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      if (hasTimestamp) {
        rows.add(new Row(header, i, new Object[] {i, i + 1, i + 2}));
      } else {
        rows.add(new Row(header, new Object[] {i, i + 1, i + 2}));
      }
    }
    return new Table(header, rows);
  }

  private Table generateTableFromValues(
      boolean hasKey, List<Field> fields, List<List<Object>> values) {
    Header header;
    if (hasKey) {
      header = new Header(Field.KEY, fields);
    } else {
      header = new Header(fields);
    }
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      if (hasKey) {
        rows.add(new Row(header, i + 1, values.get(i).toArray()));
      } else {
        rows.add(new Row(header, values.get(i).toArray()));
      }
    }
    return new Table(header, rows);
  }

  private void assertStreamEqual(RowStream a, RowStream b) throws PhysicalException {
    Header headerA = a.getHeader();
    Header headerB = b.getHeader();
    assertEquals(headerA.getFieldSize(), headerB.getFieldSize());
    for (int i = 0; i < headerA.getFieldSize(); i++) {
      assertEquals(headerA.getField(i).getName(), headerB.getField(i).getName());
      assertEquals(headerA.getField(i).getType(), headerB.getField(i).getType());
    }

    while (a.hasNext() && b.hasNext()) {
      Row rowA = a.next();
      Row rowB = b.next();
      for (int i = 0; i < headerA.getFieldSize(); i++) {
        assertEquals(rowA.getValue(i), rowB.getValue(i));
      }
    }

    if (a.hasNext() || b.hasNext()) {
      // one of the streams has not been consumed.
      fail();
    }
  }

  @Test
  public void testCrossJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(Arrays.asList(2, 2.1, true), Arrays.asList(3, 3.1, false)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(Arrays.asList(1, 1.1, true), Arrays.asList(3, 3.1, false)));

    {
      tableA.reset();
      tableB.reset();

      CrossJoin crossJoin =
          new CrossJoin(EmptySource.EMPTY_SOURCE, EmptySource.EMPTY_SOURCE, "a", "b");

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.key", DataType.LONG),
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("b.key", DataType.LONG),
                  new Field("b.a", DataType.INTEGER),
                  new Field("b.b", DataType.DOUBLE),
                  new Field("b.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(1L, 2, 2.1, true, 1L, 1, 1.1, true),
                  Arrays.asList(1L, 2, 2.1, true, 2L, 3, 3.1, false),
                  Arrays.asList(2L, 3, 3.1, false, 1L, 1, 1.1, true),
                  Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false)));

      RowStream stream = getExecutor().executeBinaryOperator(crossJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }
  }

  @Test
  public void testInnerJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(3, 3.2, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.k", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(3, 3.2, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true)));

    Table target =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.k", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(2L, 3, 3.1, false, 3L, 3, 3.2, false),
                Arrays.asList(3L, 3, 3.2, false, 2L, 3, 3.1, false),
                Arrays.asList(3L, 3, 3.2, false, 3L, 3, 3.2, false),
                Arrays.asList(5L, 5, 5.1, false, 4L, 5, 5.1, true)));

    Table usingTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.k", DataType.INTEGER),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, false),
                Arrays.asList(3L, 3, 3.2, false, 3L, 3, false),
                Arrays.asList(5L, 5, 5.1, false, 4L, 5, true)));

    {
      // NestedLoopJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.a", Op.E, "b.k"),
              Collections.emptyList(),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // HashJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.a", Op.E, "b.k"),
              Collections.emptyList(),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // SortedMergeJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.a", Op.E, "b.k"),
              Collections.emptyList(),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // using & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      usingTarget.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTarget);
    }

    {
      // using & HashJoin
      tableA.reset();
      tableB.reset();
      usingTarget.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTarget);
    }

    {
      // using & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      usingTarget.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTarget);
    }
  }

  @Test
  public void testOuterJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true),
                Arrays.asList(11, 11.1, false),
                Arrays.asList(12, 12.1, true),
                Arrays.asList(13, 13.1, false)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true),
                Arrays.asList(16, 16.1, false),
                Arrays.asList(17, 17.1, true),
                Arrays.asList(18, 18.1, false)));

    Table leftTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null, null),
                Arrays.asList(6L, 11, 11.1, false, null, null, null, null),
                Arrays.asList(7L, 12, 12.1, true, null, null, null, null),
                Arrays.asList(8L, 13, 13.1, false, null, null, null, null)));

    Table rightTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(null, null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, null, 5L, 9, 9.1, true),
                Arrays.asList(null, null, null, null, 6L, 16, 16.1, false),
                Arrays.asList(null, null, null, null, 7L, 17, 17.1, true),
                Arrays.asList(null, null, null, null, 8L, 18, 18.1, false)));

    Table fullTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null, null),
                Arrays.asList(6L, 11, 11.1, false, null, null, null, null),
                Arrays.asList(7L, 12, 12.1, true, null, null, null, null),
                Arrays.asList(8L, 13, 13.1, false, null, null, null, null),
                Arrays.asList(null, null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, null, 5L, 9, 9.1, true),
                Arrays.asList(null, null, null, null, 6L, 16, 16.1, false),
                Arrays.asList(null, null, null, null, 7L, 17, 17.1, true),
                Arrays.asList(null, null, null, null, 8L, 18, 18.1, false)));

    {
      // left & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // left & HashJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // left & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // right & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }

    {
      // right & HashJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }

    {
      // right & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }

    {
      // full & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      fullTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.FULL,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, fullTarget);
    }

    {
      // full & HashJoin
      tableA.reset();
      tableB.reset();
      fullTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.FULL,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, fullTarget);
    }

    {
      // full & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      fullTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.FULL,
              new PathFilter("a.a", Op.E, "b.a"),
              Collections.emptyList(),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, fullTarget);
    }
  }

  @Test
  public void testNaturalJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true)));

    Table target =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5.1, true)));

    Table leftTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null)));

    Table rightTarget =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, 5L, 9, 9.1, true)));

    {
      // inner & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // inner & HashJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // inner & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      target.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      // left & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // left & HashJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // left & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      leftTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.LEFT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, leftTarget);
    }

    {
      // right & NestedLoopJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }

    {
      // right & HashJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }

    {
      // right & SortedMergeJoin
      tableA.reset();
      tableB.reset();
      rightTarget.reset();

      OuterJoin outerJoin =
          new OuterJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              OuterJoinType.RIGHT,
              null,
              Collections.emptyList(),
              true,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB, null);
      assertStreamEqual(stream, rightTarget);
    }
  }

  @Test
  public void testJoinWithTypeCast() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.0, true),
                Arrays.asList(3, 3.0, false),
                Arrays.asList(4, 4.0, true),
                Arrays.asList(5, 5.0, false),
                Arrays.asList(6, 6.0, true)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.b", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(1, 1.0, true),
                Arrays.asList(3, 3.0, false),
                Arrays.asList(5, 5.0, true),
                Arrays.asList(7, 7.0, false),
                Arrays.asList(9, 9.0, true)));

    Table targetInner =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.b", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.0, false, 2L, 3, 3.0, false),
                Arrays.asList(4L, 5, 5.0, false, 3L, 5, 5.0, true)));

    Table usingTargetInner =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.0, false, 2L, 3.0, false),
                Arrays.asList(4L, 5, 5.0, false, 3L, 5.0, true)));

    {
      // NestedLoopJoin
      tableA.reset();
      tableB.reset();
      targetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.b", Op.E, "b.b"),
              Collections.emptyList(),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, targetInner);
    }

    {
      // HashJoin
      tableA.reset();
      tableB.reset();
      targetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.b", Op.E, "b.b"),
              Collections.emptyList(),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, targetInner);
    }

    {
      // SortedMergeJoin
      tableA.reset();
      tableB.reset();
      targetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              new PathFilter("a.b", Op.E, "b.b"),
              Collections.emptyList(),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, targetInner);
    }

    {
      // NestedLoopJoin
      tableA.reset();
      tableB.reset();
      usingTargetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.NestedLoopJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTargetInner);
    }

    {
      // HashJoin
      tableA.reset();
      tableB.reset();
      usingTargetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.HashJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTargetInner);
    }

    {
      // SortedMergeJoin
      tableA.reset();
      tableB.reset();
      usingTargetInner.reset();

      InnerJoin innerJoin =
          new InnerJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              "a",
              "b",
              null,
              Collections.singletonList("b"),
              false,
              JoinAlgType.SortedMergeJoin);

      RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB, null);
      assertStreamEqual(stream, usingTargetInner);
    }
  }

  @Test
  public void testSingleJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.0, true),
                Arrays.asList(3, 3.0, false),
                Arrays.asList(4, 4.0, true),
                Arrays.asList(5, 5.0, false),
                Arrays.asList(6, 6.0, true)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(new Field("b.b", DataType.INTEGER), new Field("b.e", DataType.BOOLEAN)),
            Collections.singletonList(Arrays.asList(1, true)));

    Table tableC =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("c.a", DataType.INTEGER),
                new Field("c.b", DataType.DOUBLE),
                new Field("c.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(2, 2.0, false),
                Arrays.asList(3, 3.0, true),
                Arrays.asList(4, 4.0, false),
                Arrays.asList(5, 5.0, true),
                Arrays.asList(6, 6.0, false)));

    {
      tableA.reset();
      tableB.reset();

      SingleJoin singleJoin =
          new SingleJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              new BoolFilter(true),
              JoinAlgType.NestedLoopJoin);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("b.b", DataType.INTEGER),
                  new Field("b.e", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(2, 2.0, true, 1, true),
                  Arrays.asList(3, 3.0, false, 1, true),
                  Arrays.asList(4, 4.0, true, 1, true),
                  Arrays.asList(5, 5.0, false, 1, true),
                  Arrays.asList(6, 6.0, true, 1, true)));

      RowStream stream = getExecutor().executeBinaryOperator(singleJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      tableA.reset();
      tableC.reset();

      SingleJoin singleJoin =
          new SingleJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              new PathFilter("a.b", Op.E, "c.b"),
              JoinAlgType.HashJoin);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("c.a", DataType.INTEGER),
                  new Field("c.b", DataType.DOUBLE),
                  new Field("c.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(2, 2.0, true, 2, 2.0, false),
                  Arrays.asList(3, 3.0, false, 3, 3.0, true),
                  Arrays.asList(4, 4.0, true, 4, 4.0, false),
                  Arrays.asList(5, 5.0, false, 5, 5.0, true),
                  Arrays.asList(6, 6.0, true, 6, 6.0, false)));

      RowStream stream = getExecutor().executeBinaryOperator(singleJoin, tableA, tableC, null);
      assertStreamEqual(stream, target);
    }
  }

  @Test
  public void testMarkJoin() throws PhysicalException {
    Table tableA =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3, 3.0, true),
                Arrays.asList(4, 4.0, false),
                Arrays.asList(5, 5.0, true),
                Arrays.asList(6, 6.0, false),
                Arrays.asList(7, 7.0, true)));

    Table tableB =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(1, 1.0, false),
                Arrays.asList(2, 2.0, true),
                Arrays.asList(3, 3.0, false),
                Arrays.asList(4, 4.0, true),
                Arrays.asList(5, 5.0, false)));

    {
      tableA.reset();
      tableB.reset();

      MarkJoin markJoin =
          new MarkJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              new PathFilter("a.b", Op.L, "b.b"),
              "&mark0",
              false,
              JoinAlgType.NestedLoopJoin);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("&mark0", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3, 3.0, true, true),
                  Arrays.asList(4, 4.0, false, true),
                  Arrays.asList(5, 5.0, true, false),
                  Arrays.asList(6, 6.0, false, false),
                  Arrays.asList(7, 7.0, true, false)));

      RowStream stream = getExecutor().executeBinaryOperator(markJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      tableA.reset();
      tableB.reset();

      MarkJoin markJoin =
          new MarkJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              new PathFilter("a.b", Op.E, "b.b"),
              "&mark0",
              false,
              JoinAlgType.HashJoin);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("&mark0", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3, 3.0, true, true),
                  Arrays.asList(4, 4.0, false, true),
                  Arrays.asList(5, 5.0, true, true),
                  Arrays.asList(6, 6.0, false, false),
                  Arrays.asList(7, 7.0, true, false)));

      RowStream stream = getExecutor().executeBinaryOperator(markJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }

    {
      tableA.reset();
      tableB.reset();

      MarkJoin markJoin =
          new MarkJoin(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              new PathFilter("a.b", Op.E, "b.b"),
              "&mark0",
              true,
              JoinAlgType.HashJoin);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.INTEGER),
                  new Field("a.b", DataType.DOUBLE),
                  new Field("a.c", DataType.BOOLEAN),
                  new Field("&mark0", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3, 3.0, true, false),
                  Arrays.asList(4, 4.0, false, false),
                  Arrays.asList(5, 5.0, true, false),
                  Arrays.asList(6, 6.0, false, true),
                  Arrays.asList(7, 7.0, true, true)));

      RowStream stream = getExecutor().executeBinaryOperator(markJoin, tableA, tableB, null);
      assertStreamEqual(stream, target);
    }
  }

  @Test
  public void testUnion() throws PhysicalException {
    Table tableAHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3.0, 3, true),
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(7.0, 7, true)));

    Table tableBHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(3, true, 3.0),
                Arrays.asList(4, true, 4.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(2, true, 2.0),
                Arrays.asList(5, true, 5.0)));

    Table tableAHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3.0, 3, true),
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(7.0, 7, true)));

    Table tableBHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(3, true, 3.0),
                Arrays.asList(4, true, 4.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(2, true, 2.0),
                Arrays.asList(5, true, 5.0)));

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Union union =
          new Union(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(7.0, 7, true),
                  Arrays.asList(3, 3.0, true),
                  Arrays.asList(4, 4.0, true),
                  Arrays.asList(1, 1.0, false),
                  Arrays.asList(2, 2.0, true),
                  Arrays.asList(5, 5.0, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(union, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Union union =
          new Union(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(7.0, 7, true),
                  Arrays.asList(4, 4.0, true),
                  Arrays.asList(1, 1.0, false),
                  Arrays.asList(2, 2.0, true),
                  Arrays.asList(5, 5.0, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(union, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Union union =
          new Union(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(7.0, 7, true),
                  Arrays.asList(3, 3.0, true),
                  Arrays.asList(4, 4.0, true),
                  Arrays.asList(1, 1.0, false),
                  Arrays.asList(2, 2.0, true),
                  Arrays.asList(5, 5.0, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(union, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Union union =
          new Union(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(7.0, 7, true),
                  Arrays.asList(4, 4.0, true),
                  Arrays.asList(1, 1.0, false),
                  Arrays.asList(2, 2.0, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(union, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }
  }

  @Test
  public void testExcept() throws PhysicalException {
    Table tableAHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(3.0, 3, true)));

    Table tableBHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(4, true, 4.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(2, true, 2.0),
                Arrays.asList(5, true, 5.0),
                Arrays.asList(3, true, 3.0)));

    Table tableAHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3.0, 3, true),
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(6.0, 6, false)));

    Table tableBHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(3, true, 3.0),
                Arrays.asList(4, true, 4.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(2, true, 2.0),
                Arrays.asList(5, true, 5.0)));

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Except except =
          new Except(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(6.0, 6, false)));

      RowStream stream =
          getExecutor().executeBinaryOperator(except, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Except except =
          new Except(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(6.0, 6, false)));

      RowStream stream =
          getExecutor().executeBinaryOperator(except, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Except except =
          new Except(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(4.0, 4, false),
                  Arrays.asList(6.0, 6, false),
                  Arrays.asList(6.0, 6, false)));

      RowStream stream =
          getExecutor().executeBinaryOperator(except, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Except except =
          new Except(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(Arrays.asList(4.0, 4, false), Arrays.asList(6.0, 6, false)));

      RowStream stream =
          getExecutor().executeBinaryOperator(except, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }
  }

  @Test
  public void testIntersect() throws PhysicalException {
    Table tableAHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3.0, 3, true),
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(3.0, 3, true)));

    Table tableBHasKey =
        generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(3, true, 3.0),
                Arrays.asList(4, true, 4.0),
                Arrays.asList(5, true, 5.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(3, true, 3.0)));

    Table tableAHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.a", DataType.DOUBLE),
                new Field("a.b", DataType.INTEGER),
                new Field("a.c", DataType.BOOLEAN)),
            Arrays.asList(
                Arrays.asList(3.0, 3, true),
                Arrays.asList(4.0, 4, false),
                Arrays.asList(5.0, 5, true),
                Arrays.asList(6.0, 6, false),
                Arrays.asList(3.0, 3, true)));

    Table tableBHasNoKey =
        generateTableFromValues(
            false,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.BOOLEAN),
                new Field("b.c", DataType.DOUBLE)),
            Arrays.asList(
                Arrays.asList(3, true, 3.0),
                Arrays.asList(4, true, 4.0),
                Arrays.asList(1, false, 1.0),
                Arrays.asList(2, true, 2.0),
                Arrays.asList(5, true, 5.0)));

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Intersect intersect =
          new Intersect(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(3.0, 3, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(intersect, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasKey.reset();
      tableBHasKey.reset();

      Intersect intersect =
          new Intersect(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              true,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(3.0, 3, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(intersect, tableAHasKey, tableBHasKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Intersect intersect =
          new Intersect(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              false);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(
                  Arrays.asList(3.0, 3, true),
                  Arrays.asList(5.0, 5, true),
                  Arrays.asList(3.0, 3, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(intersect, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }

    {
      tableAHasNoKey.reset();
      tableBHasNoKey.reset();

      Intersect intersect =
          new Intersect(
              EmptySource.EMPTY_SOURCE,
              EmptySource.EMPTY_SOURCE,
              Arrays.asList("a.a", "a.b", "a.c"),
              Arrays.asList("b.a", "b.c", "b.b"),
              true);

      Table target =
          generateTableFromValues(
              false,
              Arrays.asList(
                  new Field("a.a", DataType.DOUBLE),
                  new Field("a.b", DataType.INTEGER),
                  new Field("a.c", DataType.BOOLEAN)),
              Arrays.asList(Arrays.asList(3.0, 3, true), Arrays.asList(5.0, 5, true)));

      RowStream stream =
          getExecutor().executeBinaryOperator(intersect, tableAHasNoKey, tableBHasNoKey, null);
      assertStreamEqual(stream, target);
    }
  }

  // for debug
  private Table transformToTable(RowStream stream) throws PhysicalException {
    if (stream instanceof Table) {
      return (Table) stream;
    }
    Header header = stream.getHeader();
    List<Row> rows = new ArrayList<>();
    while (stream.hasNext()) {
      rows.add(stream.next());
    }
    stream.close();
    return new Table(header, rows);
  }

  @Test
  public void testProjectWithPattern() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Project project =
        new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.*"), null);
    RowStream stream = getExecutor().executeUnaryOperator(project, table, null);

    Header targetHeader = stream.getHeader();
    assertTrue(targetHeader.hasKey());
    assertEquals(2, targetHeader.getFields().size());
    assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
    assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
    assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
    assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());

    int index = 0;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row.getKey(), targetRow.getKey());
      assertEquals(row.getValue(0), targetRow.getValue(0));
      assertEquals(row.getValue(2), targetRow.getValue(1));
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testReorder() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);

    // without wildcast
    {
      table.reset();
      Reorder reorder =
          new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.a.b", "a.a.c", "a.b.c"));
      RowStream stream = getExecutor().executeUnaryOperator(reorder, table, null);
      Header targetHeader = stream.getHeader();
      assertTrue(targetHeader.hasKey());
      assertEquals(3, targetHeader.getFields().size());
      assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
      assertEquals("a.a.c", targetHeader.getFields().get(1).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
      assertEquals("a.b.c", targetHeader.getFields().get(2).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());

      int index = 0;
      while (stream.hasNext()) {
        Row targetRow = stream.next();
        assertEquals(index, targetRow.getKey());
        assertEquals(index, targetRow.getValue(0));
        assertEquals(index + 2, targetRow.getValue(1));
        assertEquals(index + 1, targetRow.getValue(2));
        index++;
      }
      assertEquals(table.getRowSize(), index);
    }

    // with wildcast 1
    {
      table.reset();
      Reorder reorder = new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.a.*", "a.b.c"));
      RowStream stream = getExecutor().executeUnaryOperator(reorder, table, null);
      Header targetHeader = stream.getHeader();
      assertTrue(targetHeader.hasKey());
      assertEquals(3, targetHeader.getFields().size());
      assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
      assertEquals("a.a.c", targetHeader.getFields().get(1).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
      assertEquals("a.b.c", targetHeader.getFields().get(2).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());

      int index = 0;
      while (stream.hasNext()) {
        Row targetRow = stream.next();
        assertEquals(index, targetRow.getKey());
        assertEquals(index, targetRow.getValue(0));
        assertEquals(index + 2, targetRow.getValue(1));
        assertEquals(index + 1, targetRow.getValue(2));
        index++;
      }
      assertEquals(table.getRowSize(), index);
    }

    // with wildcast 2
    {
      table.reset();
      Reorder reorder = new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.*", "a.b.c"));
      RowStream stream = getExecutor().executeUnaryOperator(reorder, table, null);
      Header targetHeader = stream.getHeader();
      assertTrue(targetHeader.hasKey());
      assertEquals(4, targetHeader.getFields().size());
      assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
      assertEquals("a.a.c", targetHeader.getFields().get(1).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
      assertEquals("a.b.c", targetHeader.getFields().get(2).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());
      assertEquals("a.b.c", targetHeader.getFields().get(3).getFullName());
      assertEquals(DataType.INTEGER, targetHeader.getFields().get(3).getType());

      int index = 0;
      while (stream.hasNext()) {
        Row targetRow = stream.next();
        assertEquals(index, targetRow.getKey());
        assertEquals(index, targetRow.getValue(0));
        assertEquals(index + 2, targetRow.getValue(1));
        assertEquals(index + 1, targetRow.getValue(2));
        assertEquals(index + 1, targetRow.getValue(3));
        index++;
      }
      assertEquals(table.getRowSize(), index);
    }
  }

  @Test
  public void testProjectWithoutPattern() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Project project =
        new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.b"), null);
    RowStream stream = getExecutor().executeUnaryOperator(project, table, null);

    Header targetHeader = stream.getHeader();
    assertTrue(targetHeader.hasKey());
    assertEquals(1, targetHeader.getFields().size());
    assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
    assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());

    int index = 0;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row.getKey(), targetRow.getKey());
      assertEquals(row.getValue(0), targetRow.getValue(0));
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testProjectWithMixedPattern() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Project project = new Project(EmptySource.EMPTY_SOURCE, Arrays.asList("a.*.c", "a.a.b"), null);
    RowStream stream = getExecutor().executeUnaryOperator(project, table, null);

    Header targetHeader = stream.getHeader();
    assertTrue(targetHeader.hasKey());
    assertEquals(3, targetHeader.getFields().size());
    for (Field field : table.getHeader().getFields()) {
      assertTrue(targetHeader.getFields().contains(field));
    }

    int index = 0;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row.getKey(), targetRow.getKey());
      for (int i = 0; i < 3; i++) {
        assertEquals(row.getValue(i), targetRow.getValue(i));
      }
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testSelectWithTimeFilter() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Filter filter = new KeyFilter(Op.GE, 5);
    Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
    RowStream stream = getExecutor().executeUnaryOperator(select, table, null);

    assertEquals(table.getHeader(), stream.getHeader());

    int index = 5;
    while (stream.hasNext()) {
      assertEquals(table.getRow(index), stream.next());
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testSelectWithValueFilter() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Filter filter = new ValueFilter("a.a.b", Op.NE, new Value(3));
    Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
    RowStream stream = getExecutor().executeUnaryOperator(select, table, null);

    assertEquals(table.getHeader(), stream.getHeader());

    int index = 0;
    while (stream.hasNext()) {
      assertEquals(table.getRow(index), stream.next());
      index++;
      if (index == 3) {
        index++;
      }
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testSelectWithCompoundFilter() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Filter filter =
        new AndFilter(
            Arrays.asList(new KeyFilter(Op.LE, 5), new ValueFilter("a.a.b", Op.NE, new Value(3))));
    Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
    RowStream stream = getExecutor().executeUnaryOperator(select, table, null);

    assertEquals(table.getHeader(), stream.getHeader());

    int index = 0;
    while (stream.hasNext()) {
      assertEquals(table.getRow(index), stream.next());
      index++;
      if (index == 3) {
        index++;
      }
    }
    assertEquals(6, index);
  }

  @Test
  public void testSortByTimeAsc() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Sort sort =
        new Sort(
            EmptySource.EMPTY_SOURCE, Collections.singletonList(Constants.KEY), Sort.SortType.ASC);
    RowStream stream = getExecutor().executeUnaryOperator(sort, table, null);
    assertEquals(table.getHeader(), stream.getHeader());
    int index = 0;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row, targetRow);
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testSortByTimeDesc() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Table copyTable = generateTableForUnaryOperator(true);
    Sort sort =
        new Sort(
            EmptySource.EMPTY_SOURCE, Collections.singletonList(Constants.KEY), Sort.SortType.DESC);
    RowStream stream = getExecutor().executeUnaryOperator(sort, copyTable, null);
    assertEquals(table.getHeader(), stream.getHeader());
    int index = table.getRowSize();
    while (stream.hasNext()) {
      index--;
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row, targetRow);
    }
    assertEquals(0, index);
  }

  @Test
  public void testLimit() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 5, 2);
    RowStream stream = getExecutor().executeUnaryOperator(limit, table, null);
    assertEquals(table.getHeader(), stream.getHeader());
    int index = 2;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row, targetRow);
      index++;
    }
    assertEquals(7, index);
  }

  @Test
  public void testLimitWithOutOfRangeA() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 100, 2);
    RowStream stream = getExecutor().executeUnaryOperator(limit, table, null);
    assertEquals(table.getHeader(), stream.getHeader());
    int index = 2;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      Row row = table.getRow(index);
      assertEquals(row, targetRow);
      index++;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test
  public void testLimitWithOutOfRangeB() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);
    Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 100, 200);
    RowStream stream = getExecutor().executeUnaryOperator(limit, table, null);
    assertEquals(table.getHeader(), stream.getHeader());
    assertFalse(stream.hasNext());
  }

  @Test
  public void testDownsample() throws PhysicalException {
    Table table = generateTableForUnaryOperator(true);

    FunctionParams params = new FunctionParams(Collections.singletonList("a.a.b"));

    Downsample downsample =
        new Downsample(
            EmptySource.EMPTY_SOURCE,
            3,
            3,
            Collections.singletonList(new FunctionCall(Avg.getInstance(), params)),
            new KeyRange(0, 11));
    RowStream stream = getExecutor().executeUnaryOperator(downsample, table, null);

    Header targetHeader = stream.getHeader();
    assertTrue(targetHeader.hasKey());
    assertEquals(3, targetHeader.getFields().size());
    assertEquals(WINDOW_START_COL, targetHeader.getFields().get(0).getFullName());
    assertEquals(WINDOW_END_COL, targetHeader.getFields().get(1).getFullName());
    assertEquals("avg(a.a.b)", targetHeader.getFields().get(2).getFullName());
    assertEquals(DataType.DOUBLE, targetHeader.getFields().get(2).getType());

    int index = 0;
    while (stream.hasNext()) {
      Row targetRow = stream.next();
      int sum = 0;
      int cnt = 0;
      while (cnt < 3 && index + cnt < table.getRowSize()) {
        sum += (int) table.getRow(index + cnt).getValue("a.a.b");
        cnt++;
      }
      assertEquals(sum * 1.0 / cnt, (double) targetRow.getValue(2), 0.01);
      index += cnt;
    }
    assertEquals(table.getRowSize(), index);
  }

  @Test(expected = InvalidOperatorParameterException.class)
  public void testDownsampleWithoutTimestamp() throws PhysicalException {
    Table table = generateTableForUnaryOperator(false);

    FunctionParams params = new FunctionParams(Collections.singletonList("a.a.b"));

    Downsample downsample =
        new Downsample(
            EmptySource.EMPTY_SOURCE,
            3,
            3,
            Collections.singletonList(new FunctionCall(Max.getInstance(), params)),
            new KeyRange(0, 11));
    getExecutor().executeUnaryOperator(downsample, table, null);
    fail();
  }

  @Test
  public void testMappingTransform() throws PhysicalException {
    Table table = generateTableForUnaryOperator(false);

    FunctionParams params = new FunctionParams(Collections.singletonList("a.a.b"));

    MappingTransform mappingTransform =
        new MappingTransform(
            EmptySource.EMPTY_SOURCE,
            Collections.singletonList(new FunctionCall(Last.getInstance(), params)));

    RowStream stream = getExecutor().executeUnaryOperator(mappingTransform, table, null);

    Header targetHeader = stream.getHeader();
    assertTrue(targetHeader.hasKey());
    assertEquals(2, targetHeader.getFields().size());
    assertEquals("path", targetHeader.getFields().get(0).getFullName());
    assertEquals(DataType.BINARY, targetHeader.getFields().get(0).getType());
    assertEquals("value", targetHeader.getFields().get(1).getFullName());
    assertEquals(DataType.BINARY, targetHeader.getFields().get(1).getType());

    assertTrue(stream.hasNext());

    Row targetRow = stream.next();
    Row row = table.getRow(table.getRowSize() - 1);
    assertEquals(row.getKey(), targetRow.getKey());
    assertEquals("a.a.b", targetRow.getAsValue("path").getBinaryVAsString());
    assertEquals("9", targetRow.getAsValue("value").getBinaryVAsString());
    assertFalse(stream.hasNext());
  }

  @Test
  public void testSetTransform() throws PhysicalException {
    Table table = generateTableForUnaryOperator(false);

    FunctionParams params = new FunctionParams(Collections.singletonList("a.a.b"));

    SetTransform setTransform =
        new SetTransform(
            EmptySource.EMPTY_SOURCE,
            Collections.singletonList(new FunctionCall(Avg.getInstance(), params)));

    RowStream stream = getExecutor().executeUnaryOperator(setTransform, table, null);

    Header targetHeader = stream.getHeader();
    assertFalse(targetHeader.hasKey());
    assertEquals(1, targetHeader.getFields().size());
    assertEquals("avg(a.a.b)", targetHeader.getFields().get(0).getFullName());
    assertEquals(DataType.DOUBLE, targetHeader.getFields().get(0).getType());

    assertTrue(stream.hasNext());

    Row targetRow = stream.next();

    int sum = 0;
    int cnt = 0;
    while (cnt < table.getRowSize()) {
      sum += (int) table.getRow(cnt).getValue("a.a.b");
      cnt++;
    }
    assertEquals(sum * 1.0 / cnt, (double) targetRow.getValue(0), 0.01);

    assertFalse(stream.hasNext());
  }
}
